import time
import os
import docker
import os 
import subprocess
counter = 0
def run_performance_test():
    print("Running performance test...\n")
    bash_script_path = 'ProducerConsumer/ActivityWatcher/pgbench_run.sh'    
    if not os.path.exists(bash_script_path):
        raise FileNotFoundError(f"The file {bash_script_path} does not exist.")
    
    try:
        # Ensure the script has executable permissions
        subprocess.run(['chmod', '+x', bash_script_path], check=True)
        
        # Run the script in the background
        process = subprocess.Popen(
            ['bash', bash_script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True  # Start a new session to run in the background
        )
        
        # Wait for the process to complete
        stdout, stderr = process.communicate()
        
        # Print the output and check for errors
        if process.returncode == 0:
            print("Script output:")
            print(stdout)
        else:
            print(f"Error running the script:\n{stderr}")
    
    except FileNotFoundError as e:
        print(f"File error: {str(e)}")
    except subprocess.CalledProcessError as e:
        print(f"Subprocess error: {str(e)}")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
def DBStressMonitor():
    print("Running DB Stress Monitor.....   ..\n")
    while True:
        try:
            run_performance_test()
        except Exception as e:
            print(f"An error occurred while running performance test: {str(e)}")
            raise e
        time.sleep(60)  


