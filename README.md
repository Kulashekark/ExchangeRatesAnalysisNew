## Steps to setup this project
cd .\exchange_rate_analysis\
python -m venv xrates_venv
pip install -r requirements.txt

## Executing this project
.\xrates_venv\Scripts\Activate.ps1
cd .\source\
python .\exchange_rates_analysis.py 
Deactivate

## Output of this execution will print the below aggregated summary, and generates a line graph file into .\graphs\ directory as shown below
![screenshot](.\Result_Summary.png)

![screenshot](.\xrates_aud_nzd_20240109_20240408_20240408230355.png)