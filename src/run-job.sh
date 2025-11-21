#!/bin/bash
#SBATCH --job-name=cloudProb     # create a short name for your job
#SBATCH --output=output_%j.txt          # Output file name with Job ID
#SBATCH --error=error_%j.txt            # Error file name with Job ID
#SBATCH --nodes=1               # node count
#SBATCH --ntasks-per-node=1         # number of tasks per node
#SBATCH --cpus-per-task=72          # cpu-cores per task (>1 if multi-threaded tasks)
#SBATCH --gres=gpu:0
#SBATCH --mem=400G                # memory per node
#SBATCH --time=0-12:30:00          # total run time limit (HH:MM:SS)
##SBATCH --mail-user=<email@newcastle.ac.uk> # Where to send email notifications

# capture start time
start_time=$(date +%s)

# Initialize conda for bash
eval "$(conda shell.bash hook)"
# Activate the conda environment
conda activate imago_env

# Print the job details
echo "Running job on $SLURM_CPUS_ON_NODE cores"
echo "Allocated memory: $SLURM_MEM_PER_NODE"

host=$(hostname -i)
echo ""
echo "To access the Dask dashboard, do:"
echo "ssh -N -L 8787:${host}:8787 <hpc_username>@<login_node>"
echo ""

# Run your Python script within the Singularity container
# apptainer exec \
#       --cleanenv \
#       --bind /nobackup/projects/bdncl34/storage:/storage \
#        /users/bvsh15/geo-miniconda3_latest.sif \
#        python main.py --config_path config_template.yaml

# Run your Python script directly (if not using Singularity) 
python main.py --config_path config.yaml

# capture end time and calculate elapsed time
end_time=$(date +%s)
elapsed=$(( end_time - start_time ))
echo "Job completed in $elapsed seconds."