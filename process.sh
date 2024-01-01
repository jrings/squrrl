set -e

# Extract the JSON from works with description
awk -F'\t' '{print $NF}' ol_dump_works_latest.txt | jq -c 'select(.description != null)' > works_with_desc.txt

# Extract author name and key
awk -F'\t' '{print $NF}' ol_dump_authors_latest.txt | jq -c '.key +", "+ .name' > author_key.txt