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

# Number of trials to average over
NUM_TRIALS=10

# Log file
LOGFILE="$OUTPUT_DIR/metrics_${TIMESTAMP}.log"
CSVFILE="$OUTPUT_DIR/metrics_${TIMESTAMP}.csv"

# Initialize CSV file
echo "experiment,algorithm,cores,jobs,uncertainty,avg_length,avg_threads,mutex_prob,sem_prob,trials,output" > "$CSVFILE"

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

# Function to run a single trial
run_single_trial() {
    local algorithm=$1
    local cores=$2
    local jobs_file=$3
    
    # Run algorithm
    local output=$(python3 createSchedule.py \
        -i "$jobs_file" \
        -a "$algorithm" \
        -n "$cores" 2>&1)
    
    if [[ $? -ne 0 ]]; then
        echo "FAILED"
        return 1
    fi
    
    # Extract metrics from output
    local efficiency=$(echo "$output" | grep "efficiency:" | sed 's/.*efficiency: *\([0-9.]*\).*/\1/')
    local predictability=$(echo "$output" | grep "predictability:" | sed 's/.*predictability: *\([0-9.]*\).*/\1/')
    local fairness=$(echo "$output" | grep "fairness:" | sed 's/.*fairness: *\([0-9.]*\).*/\1/')
    
    echo "$efficiency $predictability $fairness"
    return 0
}

# Function to run a single experiment with multiple trials
run_experiment() {
    local exp_name=$1
    local algorithm=$2
    local cores=$3
    local jobs=$4
    local uncertainty=$5
    local avg_length=$6
    local avg_threads=$7
    local mutex_prob=${8:-0.0}
    local sem_prob=${9:-0.0}
    
    log "Running ${algorithm} (${NUM_TRIALS} trials) with cores=${cores}, jobs=${jobs}, unc=${uncertainty}, len=${avg_length}, threads=${avg_threads}, mut=${mutex_prob}, sem=${sem_prob}"
    
    # Arrays to store results from all trials
    local -a eff_values=()
    local -a pred_values=()
    local -a fair_values=()
    local successful_trials=0
    
    # Run multiple trials
    for trial in $(seq 1 $NUM_TRIALS); do
        log "  Trial ${trial}/${NUM_TRIALS}..."
        
        # Generate jobs
        python3 simulateJobList.py \
            -n "$jobs" \
            -t 1000 \
            -l "$avg_length" \
            -u "$uncertainty" \
            --threads "$avg_threads" \
            --mut "$mutex_prob" \
            --sem "$sem_prob" \
            -o "$OUTPUT_DIR/temp_jobs_trial${trial}.pkl" >> "$LOGFILE" 2>&1
        
        if [[ $? -ne 0 ]]; then
            warn "Job generation failed for trial ${trial}"
            continue
        fi
        
        # Run the trial
        local result=$(run_single_trial "$algorithm" "$cores" "$OUTPUT_DIR/temp_jobs_trial${trial}.pkl")
        
        if [[ "$result" == "FAILED" ]]; then
            warn "Algorithm ${algorithm} failed on trial ${trial}"
            continue
        fi
        
        # Parse result
        read -r eff pred fair <<< "$result"
        
        # Validate metrics are numbers
        if [[ -n "$eff" ]] && [[ -n "$pred" ]] && [[ -n "$fair" ]]; then
            eff_values+=("$eff")
            pred_values+=("$pred")
            fair_values+=("$fair")
            ((successful_trials++))
        fi
    done
    
    # Check if we have any successful trials
    if [[ $successful_trials -eq 0 ]]; then
        warn "All trials failed for ${algorithm}"
        echo "$exp_name,$algorithm,$cores,$jobs,$uncertainty,$avg_length,$avg_threads,$mutex_prob,$sem_prob,0,FAILED" >> "$CSVFILE"
        return 1
    fi
    
    # Calculate averages
    local avg_eff=$(echo "${eff_values[@]}" | awk '{sum=0; for(i=1;i<=NF;i++) sum+=$i; print sum/NF}')
    local avg_pred=$(echo "${pred_values[@]}" | awk '{sum=0; for(i=1;i<=NF;i++) sum+=$i; print sum/NF}')
    local avg_fair=$(echo "${fair_values[@]}" | awk '{sum=0; for(i=1;i<=NF;i++) sum+=$i; print sum/NF}')
    
    # Calculate standard deviations
    local std_eff=$(echo "${eff_values[@]}" | awk -v avg="$avg_eff" '{sum=0; for(i=1;i<=NF;i++) sum+=($i-avg)^2; print sqrt(sum/NF)}')
    local std_pred=$(echo "${pred_values[@]}" | awk -v avg="$avg_pred" '{sum=0; for(i=1;i<=NF;i++) sum+=($i-avg)^2; print sqrt(sum/NF)}')
    local std_fair=$(echo "${fair_values[@]}" | awk -v avg="$avg_fair" '{sum=0; for(i=1;i<=NF;i++) sum+=($i-avg)^2; print sqrt(sum/NF)}')
    
    log "  Completed ${successful_trials}/${NUM_TRIALS} trials successfully"
    log "  Efficiency: ${avg_eff} ± ${std_eff}"
    log "  Predictability: ${avg_pred} ± ${std_pred}"
    log "  Fairness: ${avg_fair} ± ${std_fair}"
    
    # Save to CSV with averages
    echo "$exp_name,$algorithm,$cores,$jobs,$uncertainty,$avg_length,$avg_threads,$mutex_prob,$sem_prob,$successful_trials,eff=${avg_eff};pred=${avg_pred};fair=${avg_fair};eff_std=${std_eff};pred_std=${std_pred};fair_std=${std_fair}" >> "$CSVFILE"
    
    # Clean up temporary job files
    rm -f "$OUTPUT_DIR"/temp_jobs_trial*.pkl
    
    return 0
}

# Experiment 1: Vary cores
experiment_vary_cores() {
    log "=== Experiment 1: Varying Cores ==="
    for cores in 2 4 8 16 32 64; do
        for algo in FIFO PriorityQueue PCS Preemptive; do
            run_experiment "vary_cores" "$algo" "$cores" 100 5 10 3
        done
    done
}

# Experiment 2: Vary number of jobs
experiment_vary_jobs() {
    log "=== Experiment 2: Varying Number of Jobs ==="
    for jobs in 50 100 200 500 1000 2000; do
        for algo in FIFO PriorityQueue PCS Preemptive; do
            run_experiment "vary_jobs" "$algo" 4 "$jobs" 5 10 3
        done
    done
}

# Experiment 3: Vary uncertainty
experiment_vary_uncertainty() {
    log "=== Experiment 3: Varying Uncertainty ==="
    for unc in 0 5 10 20 40 80; do
        for algo in FIFO PriorityQueue PCS Preemptive; do
            run_experiment "vary_uncertainty" "$algo" 4 100 "$unc" 10 3
        done
    done
}

# Experiment 4: Vary average length
experiment_vary_length() {
    log "=== Experiment 4: Varying Average Job Length ==="
    for len in 5 10 20 40; do
        for algo in FIFO PriorityQueue PCS Preemptive; do
            run_experiment "vary_length" "$algo" 4 100 5 "$len" 3
        done
    done
}

# Experiment 5: Vary threads
experiment_vary_threads() {
    log "=== Experiment 5: Varying Average Thread Count ==="
    for threads in 2 4 8 16; do
        for algo in FIFO PriorityQueue PCS Preemptive; do
            run_experiment "vary_threads" "$algo" 4 100 5 10 "$threads"
        done
    done
}

# Experiment 6: Vary mutex probability
experiment_vary_mutex() {
    log "=== Experiment 6: Varying Mutex Probability ==="
    for mut in 0.0 0.1 0.2 0.3 0.5 0.7; do
        for algo in FIFO PriorityQueue PCS Preemptive; do
            run_experiment "vary_mutex" "$algo" 4 100 5 10 3 "$mut" 0.0
        done
    done
}

# Experiment 7: Vary semaphore probability
experiment_vary_semaphore() {
    log "=== Experiment 7: Varying Semaphore Probability ==="
    for sem in 0.0 0.1 0.2 0.3 0.5 0.7; do
        for algo in FIFO PriorityQueue PCS Preemptive; do
            run_experiment "vary_semaphore" "$algo" 4 100 5 10 3 0.0 "$sem"
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
        experiment_vary_mutex
        experiment_vary_semaphore
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
            mutex)
                experiment_vary_mutex
                ;;
            semaphore)
                experiment_vary_semaphore
                ;;
            *)
                echo "Usage: $0 [cores|jobs|uncertainty|length|threads|mutex|semaphore]"
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
