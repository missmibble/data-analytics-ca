"""
ForeSite Analytics — infrastructure teardown.

Destroys all AWS resources created by infra/setup.py, in reverse order:
  1. API Gateway
  2. Lambda function
  3. Bedrock Knowledge Bases (vector + structured)
  4. OpenSearch Serverless (collection + policies)
  5. Redshift Serverless (workgroup + namespace)
  6. S3 buckets (empties then deletes)
  7. IAM roles

Usage:
    python infra/teardown.py           # prompts for confirmation
    python infra/teardown.py --yes     # skips confirmation (CI/automated use)
"""

import argparse
import logging
import os
import time

import boto3
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

REGION = os.getenv("AWS_REGION", "ca-central-1")
ACCOUNT_ID = boto3.client("sts", region_name=REGION).get_caller_identity()["Account"]

S3_BUCKET_RAW  = os.getenv("S3_BUCKET_RAW",  "foresite-raw-ca")
S3_BUCKET_DOCS = os.getenv("S3_BUCKET_DOCS", "foresite-docs-ca")
REDSHIFT_NAMESPACE  = "foresite-ns"
REDSHIFT_WORKGROUP  = os.getenv("REDSHIFT_WORKGROUP", "foresite-wg")
AOSS_COLLECTION_NAME = "foresite-vectors"
VECTOR_KB_NAME      = "foresite-vector-kb"
STRUCTURED_KB_NAME  = "foresite-structured-kb"
LAMBDA_FUNCTION_NAME = "foresite-agent-api"
API_GW_NAME          = "foresite-api"
BEDROCK_ROLE_NAME    = "ForesiteBedrockKBRole"
LAMBDA_ROLE_NAME     = "ForesiteLambdaRole"

s3            = boto3.client("s3",                    region_name=REGION)
redshift      = boto3.client("redshift-serverless",   region_name=REGION)
bedrock_agent = boto3.client("bedrock-agent",         region_name=REGION)
aoss          = boto3.client("opensearchserverless",  region_name=REGION)
iam           = boto3.client("iam",                   region_name=REGION)
lambda_client = boto3.client("lambda",                region_name=REGION)
apigateway    = boto3.client("apigatewayv2",          region_name=REGION)


# ---------------------------------------------------------------------------
# Step 1 — API Gateway
# ---------------------------------------------------------------------------

def destroy_api_gateway() -> None:
    log.info("=== Step 1: API Gateway ===")
    apis = apigateway.get_apis().get("Items", [])
    target = next((a for a in apis if a["Name"] == API_GW_NAME), None)
    if not target:
        log.info("API Gateway not found — skipping.")
        return
    apigateway.delete_api(ApiId=target["ApiId"])
    log.info("Deleted API Gateway: %s (%s)", API_GW_NAME, target["ApiId"])


# ---------------------------------------------------------------------------
# Step 2 — Lambda
# ---------------------------------------------------------------------------

def destroy_lambda() -> None:
    log.info("=== Step 2: Lambda ===")
    try:
        lambda_client.delete_function(FunctionName=LAMBDA_FUNCTION_NAME)
        log.info("Deleted Lambda: %s", LAMBDA_FUNCTION_NAME)
    except lambda_client.exceptions.ResourceNotFoundException:
        log.info("Lambda not found — skipping.")


# ---------------------------------------------------------------------------
# Step 3 — Bedrock Knowledge Bases
# ---------------------------------------------------------------------------

def destroy_knowledge_bases() -> None:
    log.info("=== Step 3: Bedrock Knowledge Bases ===")
    kbs = bedrock_agent.list_knowledge_bases().get("knowledgeBaseSummaries", [])
    for name in (VECTOR_KB_NAME, STRUCTURED_KB_NAME):
        kb = next((k for k in kbs if k["name"] == name), None)
        if not kb:
            log.info("KB not found — skipping: %s", name)
            continue
        kb_id = kb["knowledgeBaseId"]
        # Delete data sources first
        sources = bedrock_agent.list_data_sources(knowledgeBaseId=kb_id).get("dataSourceSummaries", [])
        for src in sources:
            bedrock_agent.delete_data_source(knowledgeBaseId=kb_id, dataSourceId=src["dataSourceId"])
            log.info("  Deleted data source: %s", src["dataSourceId"])
        bedrock_agent.delete_knowledge_base(knowledgeBaseId=kb_id)
        log.info("Deleted KB: %s (%s)", name, kb_id)


# ---------------------------------------------------------------------------
# Step 4 — OpenSearch Serverless
# ---------------------------------------------------------------------------

def destroy_aoss() -> None:
    log.info("=== Step 4: OpenSearch Serverless ===")

    # Delete collection
    collections = aoss.list_collections(
        collectionFilters={"name": AOSS_COLLECTION_NAME}
    ).get("collectionSummaries", [])
    if collections:
        col_id = collections[0]["id"]
        aoss.delete_collection(id=col_id)
        log.info("Deleting AOSS collection %s — waiting for completion …", col_id)
        while True:
            remaining = aoss.list_collections(
                collectionFilters={"name": AOSS_COLLECTION_NAME}
            ).get("collectionSummaries", [])
            if not remaining:
                break
            log.info("  Still deleting …")
            time.sleep(10)
        log.info("AOSS collection deleted.")
    else:
        log.info("AOSS collection not found — skipping.")

    # Delete policies
    for policy_fn, list_fn, type_, names in [
        (aoss.delete_access_policy,   aoss.list_access_policies,   "data",       ["foresite-data"]),
        (aoss.delete_security_policy, aoss.list_security_policies, "network",    ["foresite-net"]),
        (aoss.delete_security_policy, aoss.list_security_policies, "encryption", ["foresite-enc"]),
    ]:
        for name in names:
            try:
                policy_fn(name=name, type=type_)
                log.info("Deleted AOSS %s policy: %s", type_, name)
            except Exception as e:
                if "not found" in str(e).lower() or "ResourceNotFoundException" in str(type(e)):
                    log.info("AOSS %s policy not found — skipping: %s", type_, name)
                else:
                    log.warning("Could not delete AOSS %s policy %s: %s", type_, name, e)


# ---------------------------------------------------------------------------
# Step 5 — Redshift Serverless
# ---------------------------------------------------------------------------

def destroy_redshift() -> None:
    log.info("=== Step 5: Redshift Serverless ===")

    # Workgroup
    try:
        redshift.delete_workgroup(workgroupName=REDSHIFT_WORKGROUP)
        log.info("Deleting workgroup %s — waiting …", REDSHIFT_WORKGROUP)
        while True:
            try:
                wg = redshift.get_workgroup(workgroupName=REDSHIFT_WORKGROUP)["workgroup"]
                log.info("  Workgroup status: %s", wg["status"])
                time.sleep(10)
            except redshift.exceptions.ResourceNotFoundException:
                break
        log.info("Workgroup deleted.")
    except redshift.exceptions.ResourceNotFoundException:
        log.info("Workgroup not found — skipping.")

    # Namespace
    try:
        redshift.delete_namespace(namespaceName=REDSHIFT_NAMESPACE)
        log.info("Deleting namespace %s — waiting …", REDSHIFT_NAMESPACE)
        while True:
            try:
                redshift.get_namespace(namespaceName=REDSHIFT_NAMESPACE)
                time.sleep(10)
            except redshift.exceptions.ResourceNotFoundException:
                break
        log.info("Namespace deleted.")
    except redshift.exceptions.ResourceNotFoundException:
        log.info("Namespace not found — skipping.")


# ---------------------------------------------------------------------------
# Step 6 — S3
# ---------------------------------------------------------------------------

def _empty_bucket(bucket: str) -> None:
    """Delete all objects and versions so the bucket can be removed."""
    paginator = s3.get_paginator("list_object_versions")
    for page in paginator.paginate(Bucket=bucket):
        objects = [
            {"Key": o["Key"], "VersionId": o["VersionId"]}
            for o in page.get("Versions", []) + page.get("DeleteMarkers", [])
        ]
        if objects:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
    # Also delete any unversioned objects
    paginator2 = s3.get_paginator("list_objects_v2")
    for page in paginator2.paginate(Bucket=bucket):
        objects = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objects:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})


def destroy_s3() -> None:
    log.info("=== Step 6: S3 ===")
    for bucket in (S3_BUCKET_RAW, S3_BUCKET_DOCS):
        try:
            log.info("Emptying bucket: %s …", bucket)
            _empty_bucket(bucket)
            s3.delete_bucket(Bucket=bucket)
            log.info("Deleted bucket: %s", bucket)
        except s3.exceptions.NoSuchBucket:
            log.info("Bucket not found — skipping: %s", bucket)


# ---------------------------------------------------------------------------
# Step 7 — IAM roles
# ---------------------------------------------------------------------------

def _delete_role(role_name: str) -> None:
    try:
        # Detach managed policies
        for policy in iam.list_attached_role_policies(RoleName=role_name).get("AttachedPolicies", []):
            iam.detach_role_policy(RoleName=role_name, PolicyArn=policy["PolicyArn"])
        # Delete inline policies
        for name in iam.list_role_policies(RoleName=role_name).get("PolicyNames", []):
            iam.delete_role_policy(RoleName=role_name, PolicyName=name)
        iam.delete_role(RoleName=role_name)
        log.info("Deleted IAM role: %s", role_name)
    except iam.exceptions.NoSuchEntityException:
        log.info("IAM role not found — skipping: %s", role_name)


def destroy_iam() -> None:
    log.info("=== Step 7: IAM ===")
    _delete_role(BEDROCK_ROLE_NAME)
    _delete_role(LAMBDA_ROLE_NAME)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(skip_confirm: bool = False) -> None:
    if not skip_confirm:
        print("\nThis will permanently destroy all ForeSite AWS resources:")
        print(f"  API Gateway: {API_GW_NAME}")
        print(f"  Lambda:      {LAMBDA_FUNCTION_NAME}")
        print(f"  Bedrock KBs: {VECTOR_KB_NAME}, {STRUCTURED_KB_NAME}")
        print(f"  AOSS:        {AOSS_COLLECTION_NAME}")
        print(f"  Redshift:    {REDSHIFT_NAMESPACE} / {REDSHIFT_WORKGROUP}")
        print(f"  S3:          {S3_BUCKET_RAW}, {S3_BUCKET_DOCS}  ← ALL DATA DELETED")
        print(f"  IAM:         {BEDROCK_ROLE_NAME}, {LAMBDA_ROLE_NAME}")
        print()
        confirm = input("Type 'destroy' to confirm: ").strip()
        if confirm != "destroy":
            print("Aborted.")
            return

    destroy_api_gateway()
    destroy_lambda()
    destroy_knowledge_bases()
    destroy_aoss()
    destroy_redshift()
    destroy_s3()
    destroy_iam()

    log.info("Teardown complete. All ForeSite AWS resources have been removed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Destroy all ForeSite AWS infrastructure")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt")
    args = parser.parse_args()
    main(skip_confirm=args.yes)
