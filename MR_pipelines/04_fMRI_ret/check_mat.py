import scipy.io
import sys

def check_mat_file(filepath):
    try:
        data = scipy.io.loadmat(filepath)
        print(f"✓ File is valid: {filepath}")
        print(f"  Keys: {[k for k in data.keys() if not k.startswith('__')]}")
        return True
    except Exception as e:
        print(f"✗ File is corrupted or unreadable: {filepath}")
        print(f"  Error: {e}")
        return False

if __name__ == "__main__":
    filepath = sys.argv[1] if len(sys.argv) > 1 else input("Enter .mat filepath: ")
    check_mat_file(filepath)