#!/bin/bash

# Check for duplicate DICOM files by comparing pixel data MD5 hash
# Requires: AFNI tools (dicom_hdr, to3d)
# Usage: ./check_duplicate_dcm_md5.sh <dcm_folder>

if [ $# -eq 0 ]; then
    echo "Usage: $0 <dcm_folder>"
    exit 1
fi

DCM_FOLDER="$1"

if [ ! -d "$DCM_FOLDER" ]; then
    echo "Error: Directory $DCM_FOLDER does not exist"
    exit 1
fi

echo "Checking for duplicate DICOM content using MD5 hash in: $DCM_FOLDER"
echo "================================================"
echo "Note: This may take a while for large datasets..."
echo ""

# Temporary file to store hash->filename mappings
TEMP_FILE=$(mktemp)

# Extract pixel data hash from each file
find "$DCM_FOLDER" -type f \( -iname "*.dcm" -o -iname "*.DCM" -o -iname "*.IMA" \) | while read -r dcm_file; do
    
    # Method 1: Calculate MD5 of the pixel data portion
    # Skip the first 1KB (typical DICOM header) and hash the rest (pixel data)
    # This is a rough approximation but works well
    pixel_hash=$(tail -c +1024 "$dcm_file" | md5sum | awk '{print $1}')
    
    # Also get some metadata for context
    inst_num=$(dicom_hdr "$dcm_file" 2>/dev/null | grep "0020 0013" | awk -F'//' '{print $2}' | sed 's/^[ \t]*//;s/[ \t]*$//')
    series_num=$(dicom_hdr "$dcm_file" 2>/dev/null | grep "0020 0011" | awk -F'//' '{print $2}' | sed 's/^[ \t]*//;s/[ \t]*$//')
    
    echo "$pixel_hash|$series_num|$inst_num|$dcm_file" >> "$TEMP_FILE"
done

echo "Total DICOM files processed: $(wc -l < "$TEMP_FILE")"
echo ""
echo "Analyzing for duplicates..."
echo ""

# Find duplicates based on pixel data hash
sort "$TEMP_FILE" | awk -F'|' '
{
    hash = $1
    series_num = $2
    inst_num = $3
    file = $4
    
    if (hash in files) {
        if (count[hash] == 1) {
            print "============================================"
            print "DUPLICATE PIXEL DATA FOUND"
            print "--------------------------------------------"
            print "MD5 Hash: " hash
            print "Series #: " series_num " | Instance #: " inst_num
            print ""
            print "  [1] " files[hash]
        }
        
        count[hash]++
        print "  [" count[hash] "] " file
        print ""
        
    } else {
        files[hash] = file
        count[hash] = 1
    }
}
END {
    has_duplicates = 0
    num_duplicate_sets = 0
    total_duplicate_files = 0
    
    for (hash in count) {
        if (count[hash] > 1) {
            has_duplicates = 1
            num_duplicate_sets++
            total_duplicate_files += count[hash]
        }
    }
    
    if (has_duplicates) {
        print "============================================"
        print ""
        print "SUMMARY:"
        print "  Duplicate pixel data sets: " num_duplicate_sets
        print "  Total duplicate files: " total_duplicate_files
        print ""
        print "These files contain identical pixel data but may have"
        print "different DICOM headers (UIDs, timestamps, etc)."
        exit 1
    } else {
        print "✓ No pixel data duplicates found!"
        exit 0
    }
}'

exit_code=$?

# Cleanup
rm -f "$TEMP_FILE"

exit $exit_code