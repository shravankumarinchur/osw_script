
---

# OSWatcher Analysis Script

This repository contains a Python script to analyze **OSWatcher** archive data.

## How to Run

Run the script using Python 3:

```bash
python3 script.py
```

The script will ask for the absolute path to the OSWatcher archive directory. Example:

```bash
Enter the absolute path to the OSWatcher archive directory: /var/log/oswatcher/archive/
```

Once provided, the script will prepare the required archives:

```
⏳ Preparing all required archives...

--- Checking 'oswtop' directory ---
--- Checking 'oswvmstat' directory ---
--- Checking 'oswmeminfo' directory ---
--- Checking 'oswiostat' directory ---
--- Checking 'oswnetstat' directory ---

✅ All archives are ready for analysis.
```

## Menu Options

After initialization, you will see the following menu:

```
========== OSWatcher Analysis Menu ==========
1. Check CPU performance only
2. Check Memory performance only
3. Check vmstat
4. Analyze D-state and High CPU/Memory Processes
5. Analyze Disk and IOwait
6. Analyze Netstat
7. Run All Analyses
8. Exit
Enter your choice (1-8): 
```

Choose an option to run the corresponding analysis.

## Output

* The analyzed results will be saved as an output file.
* The output file will be generated in the same directory where the OSWatcher archive data resides.

---

