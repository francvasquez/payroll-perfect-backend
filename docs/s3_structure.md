s3://pp-client-data/                          # S3_BUCKET (env, default pp-client-data)
    ├── debug_outputs/                        # debug_to_s3(); often a separate debug bucket
    │    └── debug_{employee_id}_{YYYYMMDD_HHMMSS}.csv
    └── clients/
         └── {client_id}/
              ├── config.json                 # client rules (get/save-client-config)
              ├── raw/
              │    └── {pay_date}/            # YYYY-MM-DD; original Excel uploads
              │         ├── ta.xlsx           # standardized name from frontend
              │         └── wfn.xlsx
              ├── csv/
              │    └── {pay_date}/            # parsed copies written after processing
              │         ├── ta.csv
              │         └── wfn.csv
              ├── processed/
              │    └── {pay_date}/
              │         ├── results.json      # frontend payload after processing
              │         └── annotations.json  # optional; deleted on reprocess
              └── waiver/                     # one set per client, not per pay date
                   ├── waiver.xlsx            # original Excel upload (standardized name)
                   ├── waiver.csv             # parsed copy (reused when no new waiver uploaded)
                   ├── waiver.json
                   └── waiver_meta.json       # { fileName, updatedAt } for sticky-reuse UI

Notes
- Frontend uploads Excel via presigned URL and always uses the standardized names above
  for S3 keys (ta.xlsx / wfn.xlsx / waiver.xlsx). The original waiver filename is stored
  in waiver_meta.json for display. TA and WFN go under raw/{pay_date}/; waiver goes
  under waiver/.
- After processing, TA/WFN are also saved as CSV under csv/{pay_date}/. Waiver is
  saved as both CSV and JSON next to the Excel. process-files reuses waiver.csv when
  the user does not upload a new waiver for that run.
- list-pay-periods walks clients/{client_id}/processed/ and reads metadata from
  each results.json.
- Deleting a pay period removes that date under processed/, raw/, and csv/.
  Waiver files and config.json are left in place.
