import sys
print("--- Python sys.path: ---")
for p in sys.path:
    print(p)
print("--- End sys.path ---")

print("\n--- Attempting to import runwayml ---")
try:
    import runwayml
    print("SUCCESS: 'import runwayml' executed.")
    print(f"runwayml module location: {runwayml.__file__}")
    print("\n--- Attributes of runwayml module (dir(runwayml)): ---")
    print(dir(runwayml))
    print("--- End of dir(runwayml) ---")

    print("\n--- Attempting to access RunwayML class ---")
    if hasattr(runwayml, 'RunwayML'):
        print("SUCCESS: runwayml.RunwayML is accessible.")
        from runwayml import RunwayML
        print("SUCCESS: 'from runwayml import RunwayML' executed.")
    else:
        print("FAILURE: runwayml.RunwayML is NOT accessible via attribute.")

    print("\n--- Attempting to access RunwayError class ---")
    if hasattr(runwayml, 'RunwayError'):
        print("SUCCESS: runwayml.RunwayError is accessible.")
        from runwayml import RunwayError
        print("SUCCESS: 'from runwayml import RunwayError' executed.")
    elif hasattr(runwayml, 'exceptions') and hasattr(runwayml.exceptions, 'RunwayError'):
        print("SUCCESS: runwayml.exceptions.RunwayError is accessible.")
        from runwayml.exceptions import RunwayError
        print("SUCCESS: 'from runwayml.exceptions import RunwayError' executed.")
    else:
        print("FAILURE: runwayml.RunwayError (nor runwayml.exceptions.RunwayError) is NOT accessible via attribute.")

except ImportError as e:
    print(f"IMPORT ERROR: Failed to import runwayml. Error: {e}")
except Exception as e:
    print(f"OTHER ERROR during import or attribute access: {e}")

print("\n--- Test script finished ---")
