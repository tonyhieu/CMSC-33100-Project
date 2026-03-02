#!/bin/bash

# Metric Gathering Script for Scheduling Algorithms
# This script runs experiments varying different parameters

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Create output directory
OUTPUT_DIR="metrics_output"
mkdir -p "$OUTPUT_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Log file
LOGFILE="$OUTPUT_DIR/metrics_${TIMESTAMP}.log"
CSVFILE="$OUTPUT_DIR/metrics_${TIMESTAMP}.csv"

# Initialize CSV file
echo "experiment,algorithm,cores,jobs,uncertainty,avg_length,avg_threads,output" > "$CSVFILE"

log() {
    echo -e "${GREEN}[$(date +%H:%M:%S)]${NC} $1" | tee -a "$LOGFILE"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOGFILE"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1" | tee -a "$LOGFILE"
}

# Test if simulateJobList.py and createSchedule.py exist
if [[ ! -f "simulateJobList.py" ]] || [[ ! -f "createSchedule.py" ]]; then
    error "Required scripts not found in current directory"
    exit 1
fi

# Create a python symlink if it doesn't exist
if ! command -v python &> /dev/null; then
    if command -v python3 &> /dev/null; then
        warn "python not found, creating alias to python3"
        alias python=python3
    else
        error "Neither python nor python3 found"
        exit 1
    fi
fi

# Function to run a single experiment
run_experiment() {
    local exp_name=$1
    local algorithm=$2
    local cores=$3
    local jobs=$4
    local uncertainty=$5
    local avg_length=$6
    local avg_threads=$7
    
    log "Running ${algorithm} with cores=${cores}, jobs=${jobs}, unc=${uncertainty}, len=${avg_length}, threads=${avg_threads}"
    
    # Generate jobs
    python3 simulateJobList.py \
        -n "$jobs" \
        -t 1000 \
        -l "$avg_length" \
        -u "$uncertainty" \
        --threads "$avg_threads" \
        -o "$OUTPUT_DIR/temp_jobs.pkl" >> "$LOGFILE" 2>&1
    
    if [[ $? -ne 0 ]]; then
        warn "Job generation failed"
        return 1
    fi
    
    # Run algorithm
    local output=$(python3 createSchedule.py \
        -i "$OUTPUT_DIR/temp_jobs.pkl" \
        -a "$algorithm" \
        -n "$cores" 2>&1)
    
    if [[ $? -ne 0 ]]; then
        warn "Algorithm ${algorithm} failed"
        echo "$exp_name,$algorithm,$cores,$jobs,$uncertainty,$avg_length,$avg_threads,FAILED" >> "$CSVFILE"
        return 1
    fi
    
    # Extract metrics from output
    local efficiency=$(echo "$output" | grep "efficiency:" | sed 's/.*efficiency: *\([0-9.]*\).*/\1/')
    local predictability=$(echo "$output" | grep "predictability:" | sed 's/.*predictability: *\([0-9.]*\).*/\1/')
    local fairness=$(echo "$output" | grep "fairness:" | sed 's/.*fairness: *\([0-9.]*\).*/\1/')
    
    # Save to CSV
    echo "$exp_name,$algorithm,$cores,$jobs,$uncertainty,$avg_length,$avg_threads,eff=$efficiency;pred=$predictability;fair=$fairness" >> "$CSVFILE"
    
    return 0
}

# Experiment 1: Vary cores
experiment_vary_cores() {
    log "=== Experiment 1: Varying Cores ==="
    for cores in 2 4 8 16; do
        for algo in FIFO PriorityQueue PCS Preemptive; do
            run_experiment "vary_cores" "$algo" "$cores" 100 5 10 3
        done
    done
}

# Experiment 2: Vary number of jobs
experiment_vary_jobs() {
    log "=== Experiment 2: Varying Number of Jobs ==="
    for jobs in 50 100 200 500; do
        for algo in FIFO PriorityQueue PCS Preemptive; do
            run_experiment "vary_jobs" "$algo" 4 "$jobs" 5 10 3
        done
    done
}

# Experiment 3: Vary uncertainty
experiment_vary_uncertainty() {
    log "=== Experiment 3: Varying Uncertainty ==="
    for unc in 0 5 10 20; do
        for algo in FIFO PriorityQueue PCS Preemptive; do
            run_experiment "vary_uncertainty" "$algo" 4 100 "$unc" 10 3
        done
    done
}

# Experiment 4: Vary average length
experiment_vary_length() {
    log "=== Experiment 4: Varying Average Job Length ==="
    for len in 5 10 20; do
        for algo in FIFO PriorityQueue PCS Preemptive; do
            run_experiment "vary_length" "$algo" 4 100 5 "$len" 3
        done
    done
}

# Experiment 5: Vary threads
experiment_vary_threads() {
    log "=== Experiment 5: Varying Average Thread Count ==="
    for threads in 2 4 8; do
        for algo in FIFO PriorityQueue PCS Preemptive; do
            run_experiment "vary_threads" "$algo" 4 100 5 10 "$threads"
        done
    done
}

# Main execution
main() {
    log "Starting metric gathering - output will be saved to $OUTPUT_DIR"
    log "CSV file: $CSVFILE"
    log "Log file: $LOGFILE"
    
    # Run experiments based on arguments
    if [[ $# -eq 0 ]]; then
        # Run all experiments
        experiment_vary_cores
        experiment_vary_jobs
        experiment_vary_uncertainty
        experiment_vary_length
        experiment_vary_threads
    else
        case "$1" in
            cores)
                experiment_vary_cores
                ;;
            jobs)
                experiment_vary_jobs
                ;;
            uncertainty)
                experiment_vary_uncertainty
                ;;
            length)
                experiment_vary_length
                ;;
            threads)
                experiment_vary_threads
                ;;
            *)
                echo "Usage: $0 [cores|jobs|uncertainty|length|threads]"
                echo "  Run with no arguments to run all experiments"
                exit 1
                ;;
        esac
    fi
    
    log "Metric gathering complete!"
    log "Results saved to $CSVFILE"
    
    # Print summary
    echo ""
    echo "=========================================="
    echo "SUMMARY"
    echo "=========================================="
    echo "Total experiments: $(( $(wc -l < "$CSVFILE") - 1 ))"
    echo "Failed experiments: $(grep -c "FAILED" "$CSVFILE")"
    echo "CSV file: $CSVFILE"
    echo "Log file: $LOGFILE"
    echo "=========================================="
}

# Run main function
main "$@"
