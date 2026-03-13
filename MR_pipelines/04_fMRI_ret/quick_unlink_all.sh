#!/bin/bash
# unlink_all_symlinks.sh
# Removes all symbolic links in a directory and its subdirectories

if [ $# -ne 1 ]; then
    echo "Usage: $0 <directory>"
    echo "Example: $0 /DATASETS/VOTCLOC/BIDS"
    exit 1
fi

DIR="$1"

if [ ! -d "$DIR" ]; then
    echo "Error: Directory not found: $DIR"
    exit 1
fi

echo "Searching for symbolic links in: $DIR"
echo ""

# Find and count symlinks
SYMLINK_COUNT=$(find "$DIR" -type l | wc -l)

echo "Found $SYMLINK_COUNT symbolic links"

if [ $SYMLINK_COUNT -eq 0 ]; then
    echo "No symbolic links to remove."
    exit 0
fi

# Show first 10 symlinks
echo ""
echo "First 10 symbolic links:"
find "$DIR" -type l | head -10

if [ $SYMLINK_COUNT -gt 10 ]; then
    echo "... and $((SYMLINK_COUNT - 10)) more"
fi

# Ask for confirmation
echo ""
read -p "Remove all $SYMLINK_COUNT symbolic links? (y/n) " -n 1 -r
echo

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

# Remove all symbolic links
echo "Removing symbolic links..."
find "$DIR" -type l -delete

echo "Done! Removed $SYMLINK_COUNT symbolic links."