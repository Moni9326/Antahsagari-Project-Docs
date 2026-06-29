## Welcome!

This package will help you manage Pipelines and your AWS Infrastructure with the power of CDK!

You can view this package's pipeline, [PartnerDataIngestionService](https://pipelines.amazon.com/pipelines/PartnerDataIngestionService)

## Development




```bash
brazil ws create --name PartnerDataIngestionService
cd PartnerDataIngestionService
brazil ws use \
  --versionset PartnerDataIngestionService/development \
  --platform AL2_x86_64 \
  --package PartnerDataIngestionServiceCDK
cd src/PartnerDataIngestionServiceCDK
brazil-build
```

## Useful links:
* https://builderhub.corp.amazon.com/docs/native-aws/developer-guide/cdk-pipeline.html
* https://code.amazon.com/packages/BONESConstructs/blobs/HEAD/--/packages/@amzn/pipelines/README.md
* https://code.amazon.com/packages/CDKBuild/blobs/HEAD/--/README.md
* https://docs.aws.amazon.com/cdk/api/latest/versions.html

---

## PartnerEntityMap Pipeline (BDT Maestro / Cradle → ANDES)

This package defines, as code, a daily BDT Maestro / Cradle pipeline that ingests the
Partner Ordering Service (POS) DynamoDB change-data-capture (CDC) stream and publishes a
flattened, line-item-grain mapping table to ANDES.

### Data flow

```
POS DynamoDB (PartnerEntityMapTable)
  -> DynamoDB Streams CDC  -> exported as JSON to S3 (cross-account, POS-owned bucket)
    -> Cradle profile (PartnerEntityMap) reads S3, runs Spark SQL
      -> ANDES table O_PARTNER_ENTITY_MAP  (provider: gsx-expansion)
```

* **Grain:** one row per (partner_id, order_id, line_item_id).
* **Load semantics:** delta upsert. `SNAPSHOT` bundle + `MERGE` subtype, partitioned by
  `partner_id`, with `REMOVE` events emitted as tombstones (`is_deleted = 'true'`).
* **Source images:** each CDC event carries `eventName` (INSERT/MODIFY/REMOVE) plus
  `dynamodb.NewImage` and/or `dynamodb.OldImage`. The SQL coalesces both so inserts,
  updates, and deletes are all handled.

### Files in this feature

| File | Purpose |
|------|---------|
| `definitions/cradle/PartnerEntityMap/PartnerEntityMap.json` | Cradle **profile**: S3 input (`src_delta`, POS CDC bucket), explicit `sdlSchema` declaring the full DynamoDB record (both `NewImage` and `OldImage`), ANDES output (`SNAPSHOT`/`MERGE`, provider `gsx-expansion`, dynamic partition), `runAsRoleArn = $[CRADLE_RUN_AS_ROLE]`. |
| `definitions/cradle/PartnerEntityMap/PartnerEntityMap.sql` | Spark SQL transform: dedup by `orderId` (latest event wins), explode `lineItemMappings`, derive `partner_id`/`partner_order_id` by splitting `partnerOrderId` on `.`, emit tombstones for `REMOVE`, drop rows with an empty partition key. |
| `definitions/cradle/PartnerEntityMap/jobs/FE.json` | Cradle **job** (FE realm): daily at 05:00 UTC, `standardEMR5.2PooledEdxEMRHiveResource`, business partition `regionId=3`, ticketing enabled (Sev-3). |
| `definitions/andes/O_PARTNER_ENTITY_MAP/` | ANDES **prod** table (`TABLE` + `TABLE_VERSION` v1 + `v1.sql` DDL). PK `(partner_id, order_id, line_item_id)`, partitioned by `partner_id`, `stage: prod`. |
| `definitions/andes/O_PARTNER_ENTITY_MAP_beta/` | ANDES **beta** table (same schema, `stage: beta`). |
| `definitions/PartnerDataIngestionService.cfg` | Stage variables: `STAGE`, `DESC_STAGE`, and `CRADLE_RUN_AS_ROLE` per stage. |
| `BetaChangeSet.csv` / `ProdChangeSet.csv` | Generated ChangeSet artifacts (diff between code definitions and live platform state). |

### ANDES table schema (`O_PARTNER_ENTITY_MAP`, v1)

`partner_id`, `order_id`, `partner_order_id`, `purchase_id`, `partner_purchase_id`,
`line_item_id`, `partner_line_item_id`, `customer_id`, `partner_customer_id`, `metadata`,
`created_at`, `updated_at`, `event_type`, `is_deleted`, `load_timestamp`.

* Partition key: `partner_id`
* Primary key: `(partner_id, order_id, line_item_id)`
* Provider: `gsx-expansion` (`b6bf93be-0a4c-4a54-9a64-3086565ecb05`)

### Source record attribute names (camelCase)

The DynamoDB record uses camelCase attributes. The SQL and `sdlSchema` must match exactly:
`orderId`, `partnerOrderId`, `purchaseId`, `partnerPurchaseId`, `customerId`,
`partnerCustomerId`, `createdAt`, `updatedAt`, and nested
`lineItemMappings.L[].M.{lineItemId, partnerLineItemId}`.
`ApproximateCreationDateTime` is epoch **seconds**.

### Deploy & run

```bash
# Build / validate (runs SQL + ChangeSet validation)
brazil-build release

# Deploy beta (run from a Linux dev-dsk; Mac's case-insensitive FS breaks cdk synth)
brazil-build cdk deploy PartnerDataInge-Beta
```

### References

Reference pipelines (Maestro / Cradle definition-as-code structure):

* Maestro pipeline package layout (definitions, andes, cradle profiles/jobs) reference:
  [A11yScannerPlatformMaestroDataPipelineCDK](https://code.amazon.com/packages/A11yScannerPlatformMaestroDataPipelineCDK/trees/mainline)
* Reference implementation of this same PartnerEntityMap pipeline + sample DDB-stream records:
  [KettleMetricsDataPipelineCDK (kettle-traffic-poc)](https://code.amazon.com/packages/KettleMetricsDataPipelineCDK/blobs/kettle-traffic-poc/--/definitions/PartnerEntityMap/PartnerEntityMap.sql)
  &nbsp;·&nbsp;[sample-ddb-streams-events.json](https://code.amazon.com/packages/KettleMetricsDataPipelineCDK/blobs/kettle-traffic-poc/--/poc-testdata/partner-entity-map/sample-ddb-streams-events.json)

Cradle profile / `sdlSchema` (the "No such struct field OldImage" fix):

* DynamoDB stream → ANDES profile with full `NewImage`/`OldImage` SDL:
  [WWCSBADataPipelineCDK/PromotionDetails](https://code.amazon.com/packages/WWCSBADataPipelineCDK/blobs/mainline/--/definitions/PromotionDetails/FEPromotionDetailsProfile/FEPromotionDetailsProfile.json)
* SDL list-of-maps (`L` → list of `M`) syntax for `lineItemMappings`:
  [AgentStudioManagementServiceCDK](https://code.amazon.com/packages/AgentStudioManagementServiceCDK/blobs/mainline/--/lib/andes/cradle/backfill/agents/v1.template.json)
  &nbsp;·&nbsp;[AMZFCashAppControllerMaestroCDK](https://code.amazon.com/packages/AMZFCashAppControllerMaestroCDK/blobs/mainline/--/definitions/cradle/Prod/CASH_BOOKINGS_DATA/CASH_BOOKINGS_DATA.json)
* SDL `{base:list, element:{...}}` canonical syntax (parser test suite):
  [AndesSparkUnescapedtsvDatasource SDLTest.scala](https://code.amazon.com/packages/AndesSparkUnescapedtsvDatasource/blobs/mainline/--/tst/com/amazon/spark/unescaped/tsv/util/SDLTest.scala)

Source data (the camelCase attribute-name fix):

* Source DynamoDB record model `PartnerEntityMapRecord` (confirms camelCase fields):
  [PartnerOrderingService](https://code.amazon.com/packages/PartnerOrderingService)
* Source table: DynamoDB `PartnerEntityMapTable`, account `091825117707`, `us-west-2`.
* Source CDC S3 bucket: `partnerorderingservice-dd-ddbcdctos3cdcbucketd6bdb-jidbtxywubpf`.

Platform docs:

* Maestro / Cradle / ANDES: https://docs.hub.amazon.dev/
* Brazil build system: https://docs.hub.amazon.dev/brazil/

### Errors resolved during bring-up (beta)

| Error | Root cause | Fix |
|-------|-----------|-----|
| `Cradle-InvalidSql: No such struct field OldImage` | Schema inferred from INSERT-only sample had no `OldImage` | Added explicit `sdlSchema` declaring both `NewImage` and `OldImage` |
| `Cradle-FailedDataQualityCheck: __HIVE_DEFAULT_PARTITION__` | Null/empty `partner_id` partition key | Added `split(_raw_partner_order_id,'\\.')[0] <> ''` guard |
| `Cradle-FailedDataQualityCheck: empty output dataset` | Wrong attribute casing (lowercase vs real camelCase) | Corrected all attribute names to camelCase in SQL + `sdlSchema` |

Verified on beta: FE job run **Completed**; output landed in `gsx-expansion.O_PARTNER_ENTITY_MAP_beta` (v1).
