#!/bin/bash
#SBATCH --job-name=CloudProb               #Job name
#SBATCH --output=output_%j.txt          # Output file name with Job ID
#SBATCH --error=error_%j.txt            # Error file name with Job ID
#SBATCH --gres=gpu:0
#SBATCH --nodes=1
#SBATCH --mem=500GB                     # Total memory
#SBATCH --time=0-04:30:00                 # Time limit hrs:min:sec (change as needed)
##SBATCH --mail-user=behzad.valipour-shokouhi@newcastle.ac.uk # Where to send email notifications
#SBATCH --mail-type=ALL                 # When to send email notifications
#SBATCH --partition=gh
#SBATCH --account=bdncl34

# Initialize conda for bash
eval "$(conda shell.bash hook)"
# Activate the conda environment
# conda activate imago_env
# Print the job details
echo "Running job on $SLURM_CPUS_ON_NODE cores"
echo "Allocated memory: $SLURM_MEM_PER_NODE"

host=$(hostname -i)
echo ""
echo "To access the Dask dashboard, do:"
echo "ssh -N -L 8787:${host}:8787 <hpc_username>@<login_node>"
echo ""

# Run your Python script
apptainer exec \
      --cleanenv \
      --bind /nobackup/projects/bdncl34/storage:/storage \
       /users/bvsh15/geo-miniconda3_latest.sif \
       python /users/bvsh15/codebook/dask_code.py --config_path /users/bvsh15/codebook/config_template.yaml


