## Export Events from ZEBRA - Pass any date in that month
python zebra-events-export.py -p custom -d 2026-04-01

## Cancel Single Enrollment in ZEBRA - Pass enrollment Id
python cancel-unverified-enrollment.py L8GpNnzH1LU

## Cancel enrollments in ZEBRA - Pass any date in that month
python cancel-unverified-enrollments.py -p custom -d 2026-01-01

## Synchronize Data Between eIDSR and ZEBRA - Pass any date in that month
python eidsr-zebra-sync.py -p custom -d 2026-01-01



