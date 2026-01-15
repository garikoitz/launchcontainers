#!/bin/bash

# Find DICOM files with identical series/image date/time
# Usage: ./check_duplicate_acq_time.sh <dcm_folder> [--output <file>]

DCM_FOLDER=""
OUTPUT_FILE=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        *)
            DCM_FOLDER="$1"
            shift
            ;;
    esac
done

if [ -z "$DCM_FOLDER" ]; then
    echo "Usage: $0 <dcm_folder> [--output <file>]"
    echo ""
    echo "Options:"
    echo "  --output <file>    Save list of duplicate files to remove"
    exit 1
fi

if [ ! -d "$DCM_FOLDER" ]; then
    echo "Error: Directory $DCM_FOLDER does not exist"
    exit 1
fi

if ! command -v dicom_hdr &> /dev/null; then
    echo "Error: dicom_hdr command not found. Please install AFNI tools."
    exit 1
fi

echo "Checking for DICOM files with duplicate acquisition times in: $DCM_FOLDER"
echo "================================================================"
echo ""

# Temporary files
TEMP_FILE=$(mktemp)
DUPLICATES_TO_REMOVE=$(mktemp)

echo "Extracting date/time from DICOM files..."

# Extract date and time from each file
find "$DCM_FOLDER" -type f \( -iname "*.dcm" -o -iname "*.DCM" -o -iname "*.IMA" \) | while read -r dcm_file; do
    
    # Image Date/Time (0008,0023 / 0008,0033)
    img_date=$(dicom_hdr "$dcm_file" 2>/dev/null | grep "0008 0023" | awk -F'//' '{print $2}' | sed 's/^[ \t]*//;s/[ \t]*$//')
    img_time=$(dicom_hdr "$dcm_file" 2>/dev/null | grep "0008 0033" | awk -F'//' '{print $2}' | sed 's/^[ \t]*//;s/[ \t]*$//')
    
    # Fallback to Series Date/Time
    if [ -z "$img_date" ]; then
        img_date=$(dicom_hdr "$dcm_file" 2>/dev/null | grep "0008 0021" | awk -F'//' '{print $2}' | sed 's/^[ \t]*//;s/[ \t]*$//')
    fi
    if [ -z "$img_time" ]; then
        img_time=$(dicom_hdr "$dcm_file" 2>/dev/null | grep "0008 0031" | awk -F'//' '{print $2}' | sed 's/^[ \t]*//;s/[ \t]*$//')
    fi
    
    series_num=$(dicom_hdr "$dcm_file" 2>/dev/null | grep "0020 0011" | awk -F'//' '{print $2}' | sed 's/^[ \t]*//;s/[ \t]*$//')
    inst_num=$(dicom_hdr "$dcm_file" 2>/dev/null | grep "0020 0013" | awk -F'//' '{print $2}' | sed 's/^[ \t]*//;s/[ \t]*$//')
    series_desc=$(dicom_hdr "$dcm_file" 2>/dev/null | grep "0008 103e" | awk -F'//' '{print $2}' | sed 's/^[ \t]*//;s/[ \t]*$//')
    sop_uid=$(dicom_hdr "$dcm_file" 2>/dev/null | grep "0008 0018" | awk -F'//' '{print $2}' | sed 's/^[ \t]*//;s/[ \t]*$//')
    file_size=$(stat -f%z "$dcm_file" 2>/dev/null || stat -c%s "$dcm_file" 2>/dev/null)
    
    if [ ! -z "$img_date" ] && [ ! -z "$img_time" ]; then
        datetime="${img_date}_${img_time}"
        echo "$datetime|$series_num|$inst_num|$series_desc|$file_size|$sop_uid|$dcm_file" >> "$TEMP_FILE"
    fi
done

total_files=$(wc -l < "$TEMP_FILE")
echo "Total DICOM files with date/time: $total_files"
echo ""
echo "Analyzing for duplicate acquisition times..."
echo ""

# Find and display duplicates
sort "$TEMP_FILE" | awk -F'|' -v dup_file="$DUPLICATES_TO_REMOVE" '
{
    datetime = $1
    series_num = $2
    inst_num = $3
    series_desc = $4
    file_size = $5
    sop_uid = $6
    file = $7
    
    split(datetime, dt, "_")
    date = dt[1]
    time = dt[2]
    
    if (length(date) == 8) {
        formatted_date = substr(date, 1, 4) "-" substr(date, 5, 2) "-" substr(date, 7, 2)
    } else {
        formatted_date = date
    }
    
    if (length(time) >= 6) {
        formatted_time = substr(time, 1, 2) ":" substr(time, 3, 2) ":" substr(time, 5)
    } else {
        formatted_time = time
    }
    
    if (datetime in files) {
        if (count[datetime] == 1) {
            print "============================================"
            print "DUPLICATE ACQUISITION TIME FOUND"
            print "--------------------------------------------"
            print "Date: " formatted_date
            print "Time: " formatted_time
            if (series_desc != "") print "Series: " series_desc " (#" series_num ")"
            print ""
            print "  [1] KEEP: " files[datetime]
            print "      Instance: " inst_nums[datetime] " | Size: " file_sizes[datetime] " bytes"
        }
        
        count[datetime]++
        print "  [" count[datetime] "] DUPLICATE: " file
        print "      Instance: " inst_num " | Size: " file_size " bytes"
        
        if (file_sizes[datetime] == file_size) {
            print "      ✓ Size matches"
        } else {
            print "      ⚠ Size differs"
        }
        print ""
        
        # Add to removal list (keep first, remove rest)
        print file >> dup_file
        
    } else {
        files[datetime] = file
        inst_nums[datetime] = inst_num
        file_sizes[datetime] = file_size
        count[datetime] = 1
    }
}
END {
    has_duplicates = 0
    num_duplicate_times = 0
    total_duplicate_files = 0
    
    for (datetime in count) {
        if (count[datetime] > 1) {
            has_duplicates = 1
            num_duplicate_times++
            total_duplicate_files += count[datetime]
        }
    }
    
    if (has_duplicates) {
        print "============================================"
        print ""
        print "SUMMARY:"
        print "  Unique timestamps with duplicates: " num_duplicate_times
        print "  Total duplicate files found: " total_duplicate_files - num_duplicate_times
        print "  Files to keep: " num_duplicate_times
        exit 1
    } else {
        print "✓ No duplicate acquisition times found!"
        exit 0
    }
}'

exit_code=$?

# Save duplicate list if requested
if [ ! -z "$OUTPUT_FILE" ] && [ -s "$DUPLICATES_TO_REMOVE" ]; then
    cp "$DUPLICATES_TO_REMOVE" "$OUTPUT_FILE"
    echo ""
    echo "List of duplicate files to remove saved to: $OUTPUT_FILE"
    echo "You can review and delete them with:"
    echo "  cat $OUTPUT_FILE"
    echo "  # Review the list, then:"
    echo "  cat $OUTPUT_FILE | xargs rm"
fi

# Cleanup
rm -f "$TEMP_FILE" "$DUPLICATES_TO_REMOVE"

exit $exit_code