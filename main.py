from src.etl.pipeline import DataPipeline
import os

def main():
    pipeline = DataPipeline()
    
    raw_dir = 'data/raw'
    processed_dir = 'data/processed'
    
    os.makedirs(processed_dir, exist_ok=True)
    
    ap_input = os.path.join(raw_dir, 'ap_data_sample.csv')
    ap_output = os.path.join(processed_dir, 'ap_cleaned.csv')
    
    if os.path.exists(ap_input):
        print("\nProcessing AP Data...")
        pipeline.run_phase_1(ap_input, ap_output)
    else:
        print(f"Warning: {ap_input} not found.")

    ar_input = os.path.join(raw_dir, 'ar_data_sample.csv')
    ar_output = os.path.join(processed_dir, 'ar_cleaned.csv')
    
    if os.path.exists(ar_input):
        print("\nProcessing AR Data...")
        pipeline.run_phase_1(ar_input, ar_output)
    else:
        print(f"Warning: {ar_input} not found.")

if __name__ == "__main__":
    main()

