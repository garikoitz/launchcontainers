#!/bin/bash

# Check for duplicate DICOM files with detailed information
# Usage: ./check_duplicate_dcm.sh <dcm_folder>

if [ $# -eq 0 ]; then
    echo "Usage: $0 <dcm_folder>"
    exit 1
fi

DCM_FOLDER="$1"

if [ ! -d "$DCM_FOLDER" ]; then
    echo "Error: Directory $DCM_FOLDER does not exist"
    exit 1
fi

if ! command -v dicom_hdr &> /dev/null; then
    echo "Error: dicom_hdr command not found. Please install AFNI tools."
    exit 1
fi

echo "Scanning DICOM files in: $DCM_FOLDER"
echo "================================================"

# Temporary files
TEMP_FILE=$(mktemp)
DETAILS_FILE=$(mktemp)

# Extract UID and additional info from each file
find "$DCM_FOLDER" -type f \( -iname "*.dcm" -o -iname "*.DCM" -o -iname "*.IMA" \) | while read -r dcm_file; do
    # Extract SOP Instance UID (0008,0018)
    uid=$(dicom_hdr "$dcm_file" 2>/dev/null | grep "0008 0018" | awk -F'//' '{print $2}' | sed 's/^[ \t]*//;s/[ \t]*$//')
    
    if [ ! -z "$uid" ]; then
        # Get file size and modification time
        file_size=$(stat -f%z "$dcm_file" 2>/dev/null || stat -c%s "$dcm_file" 2>/dev/null)
        mod_time=$(stat -f%Sm "$dcm_file" 2>/dev/null || stat -c%y "$dcm_file" 2>/dev/null | cut -d'.' -f1)
        
        echo "$uid|$dcm_file|$file_size|$mod_time" >> "$TEMP_FILE"
    else
        echo "Warning: Could not extract UID from $dcm_file" >&2
    fi
done

echo "Total DICOM files found: $(wc -l < "$TEMP_FILE")"
echo ""
echo "Checking for duplicates..."
echo ""

# Analyze and display duplicates
sort "$TEMP_FILE" | awk -F'|' '
{
    uid = $1
    file = $2
    size = $3
    mtime = $4
    
    if (uid in files) {
        # Mark as duplicate
        if (count[uid] == 1) {
            print "============================================"
            print "DUPLICATE UID FOUND: " uid
            print "--------------------------------------------"
            print "  [1] " files[uid]
            print "      Size: " sizes[uid] " bytes"
            print "      Modified: " mtimes[uid]
            print ""
        }
        
        count[uid]++
        print "  [" count[uid] "] " file
        print "      Size: " size " bytes"
        print "      Modified: " mtime
        
        # Check if file sizes differ
        if (sizes[uid] != size) {
            print "      WARNING: File size differs from first instance!"
        }
        print ""
        
    } else {
        files[uid] = file
        sizes[uid] = size
        mtimes[uid] = mtime
        count[uid] = 1
    }
}
END {
    has_duplicates = 0
    num_duplicate_uids = 0
    total_duplicate_files = 0
    
    for (uid in count) {
        if (count[uid] > 1) {
            has_duplicates = 1
            num_duplicate_uids++
            total_duplicate_files += count[uid]
        }
    }
    
    if (has_duplicates) {
        print "============================================"
        print ""
        print "SUMMARY:"
        print "  Unique UIDs with duplicates: " num_duplicate_uids
        print "  Total files involved: " total_duplicate_files
        print ""
        print "These files have the same SOP Instance UID and are likely"
        print "the same DICOM instance uploaded multiple times."
        exit 1
    } else {
        print "✓ No duplicate files found!"
        print "  All DICOM files have unique SOP Instance UIDs."
        exit 0
    }
}'

exit_code=$?

# Cleanup
rm -f "$TEMP_FILE" "$DETAILS_FILE"

exit $exit_code