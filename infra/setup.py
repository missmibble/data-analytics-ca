"""
AWS infrastructure provisioning — idempotent, safe to re-run.

Provisions in order:
  1. S3 buckets (raw data + docs)
  2. Redshift Serverless (namespace + workgroup + schema)
  3. Bedrock Vector KB  (foresite-vector-kb)
  4. Bedrock Structured KB  (foresite-structured-kb → Redshift)
  5. Lambda function  (src/api.py packaged as ZIP)
  6. API Gateway HTTP API  (with CORS)

Outputs KB IDs and API Gateway URL — copy these into your .env file.

Usage:
    python infra/setup.py
    python infra/setup.py --skip-lambda   # provision infra only, no Lambda deploy
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import time
import zipfile
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

REGION = os.getenv("AWS_REGION", "ca-central-1")
ACCOUNT_ID = boto3.client("sts", region_name=REGION).get_caller_identity()["Account"]

S3_BUCKET_RAW = os.getenv("S3_BUCKET_RAW", "foresite-raw-ca")
S3_BUCKET_DOCS = os.getenv("S3_BUCKET_DOCS", "foresite-docs-ca")
REDSHIFT_NAMESPACE = "foresite-ns"
REDSHIFT_WORKGROUP = os.getenv("REDSHIFT_WORKGROUP", "foresite-wg")
REDSHIFT_DATABASE = os.getenv("REDSHIFT_DATABASE", "foresite")
REDSHIFT_ADMIN_USER = "foresite_admin"

VECTOR_KB_NAME = "foresite-vector-kb"
STRUCTURED_KB_NAME = "foresite-structured-kb"
LAMBDA_FUNCTION_NAME = "foresite-agent-api"
API_GW_NAME = "foresite-api"

BEDROCK_MODEL_ARN = (
    f"arn:aws:bedrock:{REGION}::foundation-model/amazon.titan-embed-text-v2:0"
)

# ---------------------------------------------------------------------------
# Clients
# ---------------------------------------------------------------------------
s3 = boto3.client("s3", region_name=REGION)
redshift = boto3.client("redshift-serverless", region_name=REGION)
redshift_data = boto3.client("redshift-data", region_name=REGION)
bedrock_agent = boto3.client("bedrock-agent", region_name=REGION)
iam = boto3.client("iam", region_name=REGION)
lambda_client = boto3.client("lambda", region_name=REGION)
apigateway = boto3.client("apigatewayv2", region_name=REGION)
aoss = boto3.client("opensearchserverless", region_name=REGION)


# ---------------------------------------------------------------------------
# Step 1 — S3
# ---------------------------------------------------------------------------

def create_bucket(name: str) -> None:
    try:
        s3.create_bucket(
            Bucket=name,
            CreateBucketConfiguration={"LocationConstraint": REGION},
        )
        log.info("Created bucket: %s", name)
    except s3.exceptions.BucketAlreadyOwnedByYou:
        log.info("Bucket already exists: %s", name)


def provision_s3() -> None:
    log.info("=== Step 1: S3 ===")
    create_bucket(S3_BUCKET_RAW)
    create_bucket(S3_BUCKET_DOCS)


# ---------------------------------------------------------------------------
# Step 2 — Redshift Serverless
# ---------------------------------------------------------------------------

def provision_redshift() -> None:
    log.info("=== Step 2: Redshift Serverless ===")

    # Namespace — only prompt for password if it doesn't already exist
    try:
        redshift.get_namespace(namespaceName=REDSHIFT_NAMESPACE)
        log.info("Redshift namespace already exists: %s", REDSHIFT_NAMESPACE)
    except redshift.exceptions.ResourceNotFoundException:
        redshift.create_namespace(
            namespaceName=REDSHIFT_NAMESPACE,
            adminUsername=REDSHIFT_ADMIN_USER,
            adminUserPassword=_redshift_password(),
            dbName=REDSHIFT_DATABASE,
        )
        log.info("Created Redshift namespace: %s", REDSHIFT_NAMESPACE)

    # Workgroup
    try:
        redshift.create_workgroup(
            workgroupName=REDSHIFT_WORKGROUP,
            namespaceName=REDSHIFT_NAMESPACE,
            baseCapacity=8,
            publiclyAccessible=False,
        )
        log.info("Created Redshift workgroup: %s", REDSHIFT_WORKGROUP)
        log.info("Waiting for workgroup to become AVAILABLE …")
        _wait_redshift_workgroup()
    except redshift.exceptions.ConflictException:
        log.info("Redshift workgroup already exists: %s", REDSHIFT_WORKGROUP)

    # Run schema DDL
    log.info("Applying schema DDL …")
    schema_sql = Path("infra/redshift_schema.sql").read_text()
    # Statements are delimited by ~~~ to avoid splitting on semicolons inside SQL
    statements = [s.strip() for s in schema_sql.split("~~~") if s.strip()]
    for stmt in statements:
        try:
            resp = redshift_data.execute_statement(
                WorkgroupName=REDSHIFT_WORKGROUP,
                Database=REDSHIFT_DATABASE,
                Sql=stmt + ";",
            )
            _wait_redshift_statement(resp["Id"])
        except Exception as exc:
            log.warning("DDL statement skipped (may already exist): %s", exc)
    log.info("Schema DDL applied.")


def _redshift_password() -> str:
    """Read from env or prompt — never hardcode."""
    pw = os.getenv("REDSHIFT_ADMIN_PASSWORD")
    if not pw:
        import getpass
        pw = getpass.getpass("Redshift admin password (min 8 chars, upper+lower+digit): ")
    return pw


def _wait_redshift_workgroup(poll: int = 10) -> None:
    while True:
        wg = redshift.get_workgroup(workgroupName=REDSHIFT_WORKGROUP)["workgroup"]
        if wg["status"] == "AVAILABLE":
            return
        log.info("  Workgroup status: %s — waiting …", wg["status"])
        time.sleep(poll)


def _wait_redshift_statement(statement_id: str, poll: int = 3) -> None:
    while True:
        desc = redshift_data.describe_statement(Id=statement_id)
        status = desc["Status"]
        if status == "FINISHED":
            return
        if status in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Statement {statement_id} {status}: {desc.get('Error')}")
        time.sleep(poll)


# ---------------------------------------------------------------------------
# Step 3 — Bedrock Vector KB
# ---------------------------------------------------------------------------

def _get_or_create_bedrock_role() -> str:
    """Create/return IAM role that Bedrock KB can assume. Always syncs the inline policy."""
    role_name = "ForesiteBedrockKBRole"
    try:
        role_arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
        log.info("Using existing IAM role: %s", role_name)
    except iam.exceptions.NoSuchEntityException:
        trust = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "bedrock.amazonaws.com"},
                "Action": "sts:AssumeRole",
                "Condition": {
                    "StringEquals": {"aws:SourceAccount": ACCOUNT_ID},
                    "ArnLike": {"aws:SourceArn": f"arn:aws:bedrock:{REGION}:{ACCOUNT_ID}:knowledge-base/*"},
                },
            }],
        }
        role_arn = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust),
            Description="Bedrock Knowledge Base access role for ForeSite Analytics",
        )["Role"]["Arn"]
        log.info("Created IAM role: %s", role_arn)

    # Always apply so policy stays current (handles roles created before aoss permission was added)
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="ForesiteBedrockKBPolicy",
        PolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "s3:ListBucket"],
                    "Resource": [
                        f"arn:aws:s3:::{S3_BUCKET_DOCS}",
                        f"arn:aws:s3:::{S3_BUCKET_DOCS}/*",
                    ],
                },
                {
                    "Effect": "Allow",
                    "Action": ["bedrock:InvokeModel", "bedrock:GenerateQuery"],
                    "Resource": "*",
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "redshift-data:ExecuteStatement", "redshift-data:DescribeStatement",
                        "redshift-data:GetStatementResult", "redshift-data:CancelStatement",
                        "redshift-serverless:GetCredentials",
                    ],
                    "Resource": "*",
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "sqlworkbench:GetSqlRecommendations",
                        "sqlworkbench:AssociateConnectionWithEnvironment",
                        "sqlworkbench:GetAutocompletionResult",
                    ],
                    "Resource": "*",
                },
                {
                    "Effect": "Allow",
                    "Action": ["aoss:APIAccessAll"],
                    "Resource": f"arn:aws:aoss:{REGION}:{ACCOUNT_ID}:collection/*",
                },
            ],
        }),
    )
    log.info("IAM policy synced for role: %s", role_name)
    time.sleep(10)  # IAM propagation
    return role_arn


AOSS_COLLECTION_NAME = "foresite-vectors"
AOSS_INDEX_NAME = "foresite-index"


def _create_aoss_policies(role_arn: str) -> None:
    """Create AOSS encryption, network, and data access policies (idempotent)."""
    enc_policy = json.dumps({
        "Rules": [{"Resource": [f"collection/{AOSS_COLLECTION_NAME}"], "ResourceType": "collection"}],
        "AWSOwnedKey": True,
    })
    net_policy = json.dumps([{
        "Rules": [
            {"Resource": [f"collection/{AOSS_COLLECTION_NAME}"], "ResourceType": "collection"},
            {"Resource": [f"collection/{AOSS_COLLECTION_NAME}"], "ResourceType": "dashboard"},
        ],
        "AllowFromPublic": True,
    }])
    data_policy = json.dumps([{
        "Rules": [
            {
                "Resource": [f"collection/{AOSS_COLLECTION_NAME}"],
                "Permission": [
                    "aoss:CreateCollectionItems", "aoss:DeleteCollectionItems",
                    "aoss:UpdateCollectionItems", "aoss:DescribeCollectionItems",
                ],
                "ResourceType": "collection",
            },
            {
                "Resource": [f"index/{AOSS_COLLECTION_NAME}/*"],
                "Permission": [
                    "aoss:CreateIndex", "aoss:DeleteIndex", "aoss:UpdateIndex",
                    "aoss:DescribeIndex", "aoss:ReadDocument", "aoss:WriteDocument",
                ],
                "ResourceType": "index",
            },
        ],
        "Principal": [role_arn, f"arn:aws:iam::{ACCOUNT_ID}:root"],
    }])

    # Encryption and network policies — create once, never need updating
    for policy_fn, kwargs in [
        (aoss.create_security_policy, {"name": "foresite-enc", "type": "encryption", "policy": enc_policy}),
        (aoss.create_security_policy, {"name": "foresite-net", "type": "network", "policy": net_policy}),
    ]:
        try:
            policy_fn(**kwargs)
            log.info("Created AOSS policy: %s", kwargs["name"])
        except aoss.exceptions.ConflictException:
            log.info("AOSS policy already exists: %s", kwargs["name"])

    # Data access policy — always upsert so the role ARN is always current
    try:
        aoss.create_access_policy(name="foresite-data", type="data", policy=data_policy)
        log.info("Created AOSS data access policy.")
    except aoss.exceptions.ConflictException:
        existing = aoss.get_access_policy(name="foresite-data", type="data")
        try:
            aoss.update_access_policy(
                name="foresite-data",
                type="data",
                policy=data_policy,
                policyVersion=existing["accessPolicyDetail"]["policyVersion"],
            )
            log.info("Updated AOSS data access policy with current role ARN.")
        except Exception as e:
            if "No changes detected" in str(e):
                log.info("AOSS data access policy already up to date.")
            else:
                raise

    # Allow time for IAM + AOSS policy propagation before Bedrock attempts access
    log.info("Waiting 15s for IAM/AOSS policy propagation …")
    time.sleep(15)


def _get_or_create_aoss_collection() -> tuple[str, str]:
    """Create AOSS collection and wait for ACTIVE. Returns (collection_id, endpoint)."""
    existing = aoss.list_collections(
        collectionFilters={"name": AOSS_COLLECTION_NAME}
    ).get("collectionSummaries", [])

    if existing:
        col = existing[0]
        log.info("AOSS collection already exists: %s", col["id"])
    else:
        resp = aoss.create_collection(name=AOSS_COLLECTION_NAME, type="VECTORSEARCH")
        col = resp["createCollectionDetail"]
        log.info("Created AOSS collection: %s — waiting for ACTIVE …", col["id"])

    collection_id = col["id"]
    # Wait for ACTIVE
    while True:
        detail = aoss.batch_get_collection(ids=[collection_id])["collectionDetails"][0]
        if detail["status"] == "ACTIVE":
            endpoint = detail["collectionEndpoint"]
            log.info("AOSS collection ACTIVE: %s", endpoint)
            return collection_id, endpoint
        if detail["status"] == "FAILED":
            raise RuntimeError(f"AOSS collection creation failed: {detail}")
        log.info("  AOSS status: %s — waiting …", detail["status"])
        time.sleep(15)


def _create_vector_index(endpoint: str) -> None:
    """Create the vector index in the AOSS collection (idempotent)."""
    import boto3.session
    from opensearchpy import OpenSearch, RequestsHttpConnection, exceptions as os_exc
    from requests_aws4auth import AWS4Auth

    credentials = boto3.session.Session().get_credentials()
    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        REGION,
        "aoss",
        session_token=credentials.token,
    )
    host = endpoint.replace("https://", "")
    client = OpenSearch(
        hosts=[{"host": host, "port": 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=30,
    )
    if client.indices.exists(index=AOSS_INDEX_NAME):
        log.info("Vector index already exists: %s", AOSS_INDEX_NAME)
        return

    index_body = {
        "settings": {"index": {"knn": True}},
        "mappings": {
            "properties": {
                "embedding":  {"type": "knn_vector", "dimension": 1024, "method": {"engine": "faiss", "name": "hnsw"}},
                "text":       {"type": "text"},
                "metadata":   {"type": "object", "enabled": False},
            }
        },
    }
    client.indices.create(index=AOSS_INDEX_NAME, body=index_body)
    log.info("Created vector index: %s", AOSS_INDEX_NAME)


def provision_vector_kb(role_arn: str) -> str:
    log.info("=== Step 3: Bedrock Vector KB ===")

    # Check if KB already exists
    kbs = bedrock_agent.list_knowledge_bases()["knowledgeBaseSummaries"]
    existing = next((kb for kb in kbs if kb["name"] == VECTOR_KB_NAME), None)
    if existing:
        kb_id = existing["knowledgeBaseId"]
        log.info("Vector KB already exists: %s (%s)", VECTOR_KB_NAME, kb_id)
        return kb_id

    # Prerequisites: AOSS policies → collection → vector index
    _create_aoss_policies(role_arn)
    collection_id, endpoint = _get_or_create_aoss_collection()
    _create_vector_index(endpoint)

    collection_arn = f"arn:aws:aoss:{REGION}:{ACCOUNT_ID}:collection/{collection_id}"

    resp = bedrock_agent.create_knowledge_base(
        name=VECTOR_KB_NAME,
        description="ForeSite vector KB — CMHC reports, methodology docs, data definitions",
        roleArn=role_arn,
        knowledgeBaseConfiguration={
            "type": "VECTOR",
            "vectorKnowledgeBaseConfiguration": {
                "embeddingModelArn": BEDROCK_MODEL_ARN,
            },
        },
        storageConfiguration={
            "type": "OPENSEARCH_SERVERLESS",
            "opensearchServerlessConfiguration": {
                "collectionArn": collection_arn,
                "vectorIndexName": AOSS_INDEX_NAME,
                "fieldMapping": {
                    "vectorField": "embedding",
                    "textField": "text",
                    "metadataField": "metadata",
                },
            },
        },
    )
    kb_id = resp["knowledgeBase"]["knowledgeBaseId"]
    log.info("Created vector KB: %s (%s)", VECTOR_KB_NAME, kb_id)

    bedrock_agent.create_data_source(
        knowledgeBaseId=kb_id,
        name="cmhc-docs-s3",
        dataSourceConfiguration={
            "type": "S3",
            "s3Configuration": {"bucketArn": f"arn:aws:s3:::{S3_BUCKET_DOCS}"},
        },
    )
    log.info("Data source added to vector KB.")
    return kb_id


# ---------------------------------------------------------------------------
# Step 4 — Bedrock Structured KB
# ---------------------------------------------------------------------------

def provision_structured_kb(role_arn: str) -> str:
    log.info("=== Step 4: Bedrock Structured KB ===")
    kbs = bedrock_agent.list_knowledge_bases()["knowledgeBaseSummaries"]
    existing = next((kb for kb in kbs if kb["name"] == STRUCTURED_KB_NAME), None)
    if existing:
        kb_id = existing["knowledgeBaseId"]
        log.info("Structured KB already exists: %s (%s)", STRUCTURED_KB_NAME, kb_id)
        return kb_id

    # Fetch the actual workgroup ARN (contains UUID, not just the name)
    workgroup_arn = redshift.get_workgroup(workgroupName=REDSHIFT_WORKGROUP)["workgroup"]["workgroupArn"]
    log.info("Redshift workgroup ARN: %s", workgroup_arn)

    resp = bedrock_agent.create_knowledge_base(
        name=STRUCTURED_KB_NAME,
        description="ForeSite structured KB — NL-to-SQL over Redshift (CPI, rent, income, NHPI)",
        roleArn=role_arn,
        knowledgeBaseConfiguration={
            "type": "SQL",
            "sqlKnowledgeBaseConfiguration": {
                "type": "REDSHIFT",
                "redshiftConfiguration": {
                    "storageConfigurations": [{
                        "type": "REDSHIFT",
                        "redshiftConfiguration": {
                            "databaseName": REDSHIFT_DATABASE,
                        },
                    }],
                    "queryEngineConfiguration": {
                        "type": "SERVERLESS",
                        "serverlessConfiguration": {
                            "workgroupArn": workgroup_arn,
                            "authConfiguration": {"type": "IAM"},
                        },
                    },
                },
            },
        },
    )
    kb_id = resp["knowledgeBase"]["knowledgeBaseId"]
    log.info("Created structured KB: %s (%s)", STRUCTURED_KB_NAME, kb_id)
    return kb_id


# ---------------------------------------------------------------------------
# Step 5 — Lambda
# ---------------------------------------------------------------------------

def _get_or_create_lambda_role() -> str:
    role_name = "ForesiteLambdaRole"
    try:
        return iam.get_role(RoleName=role_name)["Role"]["Arn"]
    except iam.exceptions.NoSuchEntityException:
        pass

    trust = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    }
    role_arn = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(trust),
    )["Role"]["Arn"]

    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    )
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="ForesiteLambdaPolicy",
        PolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": ["bedrock:*"], "Resource": "*"},
                {"Effect": "Allow", "Action": ["redshift-data:*", "redshift-serverless:GetCredentials"], "Resource": "*"},
            ],
        }),
    )
    log.info("Created Lambda IAM role: %s", role_arn)
    time.sleep(10)  # IAM propagation
    return role_arn


def _build_lambda_zip() -> bytes:
    """Package src/ + installed dependencies into a Lambda-compatible ZIP.

    Uses --python-platform linux and --python-version 3.11 to ensure
    compiled C extensions (e.g. pydantic_core) are downloaded as
    manylinux wheels compatible with Lambda's Amazon Linux environment,
    even when building from macOS.
    """
    zip_path = Path("lambda.zip")
    pkg_dir = Path("lambda_package")
    if pkg_dir.exists():
        import shutil
        shutil.rmtree(pkg_dir)
    log.info("Building Lambda deployment package (linux/x86_64 wheels) …")
    subprocess.run(
        [
            str(Path.home() / ".local/bin/uv"), "pip", "install",
            "--target", str(pkg_dir),
            "--python-platform", "linux",
            "--python-version", "3.11",
            "--no-cache",
            "--quiet",
            "strands-agents", "strands-agents-tools",
            "fastapi", "mangum", "uvicorn",
            "boto3", "python-dotenv",
        ],
        check=True,
    )
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        # Add installed packages
        for f in Path("lambda_package").rglob("*"):
            if f.is_file():
                zf.write(f, f.relative_to("lambda_package"))
        # Add application source
        for f in Path("src").rglob("*.py"):
            zf.write(f, f)
    log.info("Lambda ZIP built: %s (%.1f MB)", zip_path, zip_path.stat().st_size / 1_048_576)
    return zip_path.read_bytes()


def provision_lambda(role_arn: str, structured_kb_id: str, vector_kb_id: str) -> str:
    log.info("=== Step 5: Lambda ===")
    allowed_origins = os.getenv("ALLOWED_ORIGINS", "*")
    env_vars = {
        "REDSHIFT_WORKGROUP": REDSHIFT_WORKGROUP,
        "REDSHIFT_DATABASE": REDSHIFT_DATABASE,
        "STRUCTURED_KB_ID": structured_kb_id,
        "VECTOR_KB_ID": vector_kb_id,
        "ALLOWED_ORIGINS": allowed_origins,
    }
    zip_bytes = _build_lambda_zip()

    try:
        resp = lambda_client.get_function(FunctionName=LAMBDA_FUNCTION_NAME)
        lambda_client.update_function_code(
            FunctionName=LAMBDA_FUNCTION_NAME, ZipFile=zip_bytes
        )
        lambda_client.update_function_configuration(
            FunctionName=LAMBDA_FUNCTION_NAME, Environment={"Variables": env_vars}, MemorySize=1536
        )
        fn_arn = resp["Configuration"]["FunctionArn"]
        log.info("Updated Lambda: %s", fn_arn)
    except lambda_client.exceptions.ResourceNotFoundException:
        resp = lambda_client.create_function(
            FunctionName=LAMBDA_FUNCTION_NAME,
            Runtime="python3.11",
            Role=role_arn,
            Handler="src.api.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=120,
            MemorySize=1536,
            Environment={"Variables": env_vars},
        )
        fn_arn = resp["FunctionArn"]
        log.info("Created Lambda: %s", fn_arn)

    return fn_arn


# ---------------------------------------------------------------------------
# Step 5b — Lambda Function URL (streaming SSE)
# ---------------------------------------------------------------------------

def provision_function_url(lambda_arn: str) -> str:
    """Create or update a Lambda Function URL with InvokeMode=RESPONSE_STREAM."""
    log.info("=== Step 5b: Lambda Function URL (streaming) ===")
    allowed_origins = os.getenv("ALLOWED_ORIGINS", "*")
    origins = [o.strip() for o in allowed_origins.split(",")]

    cors_config = {
        "AllowCredentials": False,
        "AllowHeaders": ["content-type"],
        "AllowMethods": ["GET", "POST"],
        "AllowOrigins": origins,
        "MaxAge": 86400,
    }

    try:
        resp = lambda_client.get_function_url_config(FunctionName=LAMBDA_FUNCTION_NAME)
        lambda_client.update_function_url_config(
            FunctionName=LAMBDA_FUNCTION_NAME,
            Cors=cors_config,
        )
        url = resp["FunctionUrl"]
        log.info("Function URL already exists: %s", url)
    except lambda_client.exceptions.ResourceNotFoundException:
        resp = lambda_client.create_function_url_config(
            FunctionName=LAMBDA_FUNCTION_NAME,
            AuthType="NONE",
            InvokeMode="RESPONSE_STREAM",
            Cors=cors_config,
        )
        url = resp["FunctionUrl"]
        log.info("Created Function URL: %s", url)

        # Allow public (unauthenticated) access
        try:
            lambda_client.add_permission(
                FunctionName=LAMBDA_FUNCTION_NAME,
                StatementId="FunctionURLAllowPublicAccess",
                Action="lambda:InvokeFunctionUrl",
                Principal="*",
                FunctionUrlAuthType="NONE",
            )
        except lambda_client.exceptions.ResourceConflictException:
            pass  # permission already exists

    return url.rstrip("/")


# ---------------------------------------------------------------------------
# Step 6 — API Gateway
# ---------------------------------------------------------------------------

def provision_api_gateway(lambda_arn: str) -> str:
    log.info("=== Step 6: API Gateway ===")
    allowed_origins = os.getenv("ALLOWED_ORIGINS", "*")
    origins = [o.strip() for o in allowed_origins.split(",")]

    # Check if already exists
    apis = apigateway.get_apis()["Items"]
    existing = next((a for a in apis if a["Name"] == API_GW_NAME), None)

    if existing:
        api_id = existing["ApiId"]
        log.info("API Gateway already exists: %s (%s)", API_GW_NAME, api_id)
    else:
        resp = apigateway.create_api(
            Name=API_GW_NAME,
            ProtocolType="HTTP",
            CorsConfiguration={
                "AllowOrigins": origins,
                "AllowMethods": ["GET", "POST"],
                "AllowHeaders": ["Content-Type", "Authorization"],
                "MaxAge": 300,
            },
        )
        api_id = resp["ApiId"]
        log.info("Created API Gateway: %s (%s)", API_GW_NAME, api_id)

        # Lambda integration
        integration = apigateway.create_integration(
            ApiId=api_id,
            IntegrationType="AWS_PROXY",
            IntegrationUri=lambda_arn,
            PayloadFormatVersion="2.0",
        )
        integration_id = integration["IntegrationId"]

        # Routes
        for method, path in [("POST", "/chat"), ("GET", "/health")]:
            apigateway.create_route(
                ApiId=api_id,
                RouteKey=f"{method} {path}",
                Target=f"integrations/{integration_id}",
            )

        # Default stage with auto-deploy
        apigateway.create_stage(
            ApiId=api_id,
            StageName="$default",
            AutoDeploy=True,
        )

        # Grant API GW permission to invoke Lambda
        lambda_client.add_permission(
            FunctionName=LAMBDA_FUNCTION_NAME,
            StatementId="APIGatewayInvoke",
            Action="lambda:InvokeFunction",
            Principal="apigateway.amazonaws.com",
            SourceArn=f"arn:aws:execute-api:{REGION}:{ACCOUNT_ID}:{api_id}/*",
        )

    invoke_url = f"https://{api_id}.execute-api.{REGION}.amazonaws.com"
    log.info("API Gateway invoke URL: %s", invoke_url)
    return invoke_url


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(skip_lambda: bool = False) -> None:
    provision_s3()
    provision_redshift()

    role_arn = _get_or_create_bedrock_role()
    vector_kb_id = provision_vector_kb(role_arn)
    structured_kb_id = provision_structured_kb(role_arn)

    if not skip_lambda:
        lambda_role_arn = _get_or_create_lambda_role()
        lambda_arn = provision_lambda(lambda_role_arn, structured_kb_id, vector_kb_id)
        invoke_url = provision_api_gateway(lambda_arn)
        stream_url = provision_function_url(lambda_arn)
    else:
        log.info("Skipping Lambda and API Gateway (--skip-lambda).")
        invoke_url = "<not provisioned>"
        stream_url = "<not provisioned>"

    print("\n" + "=" * 60)
    print("Provisioning complete. Add these to your .env file:")
    print("=" * 60)
    print(f"STRUCTURED_KB_ID={structured_kb_id}")
    print(f"VECTOR_KB_ID={vector_kb_id}")
    print(f"API_GATEWAY_URL={invoke_url}")
    print(f"STREAM_URL={stream_url}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Provision ForeSite AWS infrastructure")
    parser.add_argument("--skip-lambda", action="store_true", help="Provision infra only, skip Lambda/API GW")
    args = parser.parse_args()
    main(skip_lambda=args.skip_lambda)
