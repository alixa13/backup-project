#!/bin/bash

set -e

# Step 0: Check current directory is a git repo
if [ ! -d ".git" ]; then
    echo "❌ Not a Git repository. Run this inside your Git project."
    exit 1
fi

# Step 1: Check or install git-filter-repo
if ! command -v git-filter-repo &> /dev/null; then
    echo "🔧 git-filter-repo not found. Attempting to install..."

    # Try install based on OS
    if command -v apt &> /dev/null; then
        sudo apt update && sudo apt install -y git-filter-repo
    elif command -v brew &> /dev/null; then
        brew install git-filter-repo
    else
        echo "❌ Install git-filter-repo manually from https://github.com/newren/git-filter-repo"
        exit 1
    fi
fi

# Step 2: Strip blobs over 10MB (10 * 1024 * 1024 = 10485760 bytes)
echo "🧹 Removing files >10MB from Git history..."
git filter-repo --strip-blobs-bigger-than 10485760 --force

# Step 3: Clean up filter-repo backup
rm -rf .git/filter-repo

# Step 4: Force push all branches and tags
echo "🚀 Force pushing cleaned repository to origin..."
git remote -v
git push origin --force --all
git push origin --force --tags

echo "✅ Done. Large files removed and history cleaned!"
