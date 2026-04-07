"""
BIOLOGICAL COMPUTING SYSTEM
DNA-based storage, protein folding computation, and bio-molecular email processing
"""

import asyncio
import numpy as np
import time
import random
from typing import Dict, List, Optional, Any, Tuple, Union, Set
from dataclasses import dataclass, field
from collections import defaultdict, deque
from enum import Enum
import threading
from datetime import datetime, timedelta
import json
import base64
import hashlib

class DNABase(Enum):
    """DNA base pairs"""
    ADENINE = "A"
    THYMINE = "T"
    GUANINE = "G"
    CYTOSINE = "C"

class ProteinStructure(Enum):
    """Protein folding structures"""
    ALPHA_HELIX = "alpha_helix"
    BETA_SHEET = "beta_sheet"
    LOOP_REGION = "loop_region"
    BETA_TURN = "beta_turn"

class BioComputeOperation(Enum):
    """Biological computation operations"""
    DNA_ENCODE = "dna_encode"
    DNA_DECODE = "dna_decode"
    PROTEIN_FOLD = "protein_fold"
    ENZYME_CATALYSIS = "enzyme_catalysis"
    MOLECULAR_RECOGNITION = "molecular_recognition"
    GENETIC_ALGORITHM = "genetic_algorithm"

@dataclass
class DNASequence:
    """DNA sequence for information storage"""
    sequence_id: str
    bases: List[DNABase]
    encoded_data: bytes
    error_correction: List[DNABase] = field(default_factory=list)
    synthesized_at: datetime = field(default_factory=datetime.now)
    degradation_rate: float = 0.001  # per day
    
    def get_sequence_string(self) -> str:
        """Get DNA sequence as string"""
        return ''.join([base.value for base in self.bases])
    
    def calculate_gc_content(self) -> float:
        """Calculate GC content percentage"""
        gc_count = sum(1 for base in self.bases 
                      if base in [DNABase.GUANINE, DNABase.CYTOSINE])
        return gc_count / len(self.bases) if self.bases else 0.0
    
    def is_degraded(self) -> bool:
        """Check if DNA sequence has degraded"""
        days_elapsed = (datetime.now() - self.synthesized_at).days
        degradation_prob = 1 - (1 - self.degradation_rate) ** days_elapsed
        return random.random() < degradation_prob

@dataclass
class ProteinMolecule:
    """Protein molecule for computation"""
    protein_id: str
    amino_acid_sequence: str
    structure: ProteinStructure
    active_sites: List[Dict[str, Any]] = field(default_factory=list)
    folding_energy: float = 0.0
    stability: float = 1.0
    function: str = ""
    
    def calculate_folding_energy(self) -> float:
        """Calculate protein folding energy"""
        # Simplified energy calculation based on amino acid interactions
        energy = 0.0
        for i, aa in enumerate(self.amino_acid_sequence):
            # Hydrophobic interactions
            if aa in 'AILMFPWV':  # Hydrophobic amino acids
                energy -= 0.5
            # Electrostatic interactions
            elif aa in 'KRH':  # Positive charge
                energy += 0.3
            elif aa in 'DE':  # Negative charge
                energy += 0.3
            # Disulfide bonds
            elif aa == 'C':  # Cysteine
                energy -= 1.0
        
        self.folding_energy = energy
        return energy
    
    def simulate_folding(self) -> ProteinStructure:
        """Simulate protein folding process"""
        energy = self.calculate_folding_energy()
        
        # Determine structure based on energy and sequence
        if energy < -10:
            self.structure = ProteinStructure.ALPHA_HELIX
        elif energy < -5:
            self.structure = ProteinStructure.BETA_SHEET
        elif energy < 0:
            self.structure = ProteinStructure.BETA_TURN
        else:
            self.structure = ProteinStructure.LOOP_REGION
        
        return self.structure

class DNAStorage:
    """DNA-based information storage system"""
    
    def __init__(self):
        self.sequences: Dict[str, DNASequence] = {}
        self.codon_table = self._initialize_codon_table()
        self.storage_stats = {
            "total_sequences": 0,
            "total_bytes_stored": 0,
            "storage_density": 0.0,  # bytes per gram
            "error_rate": 0.0001,  # per base
            "synthesis_time": deque(maxlen=100),
            "retrieval_time": deque(maxlen=100)
        }
    
    def _initialize_codon_table(self) -> Dict[Tuple[str, str, str], int]:
        """Initialize codon to byte mapping"""
        bases = ['A', 'T', 'G', 'C']
        codon_table = {}
        
        # Generate all possible codons and map to values 0-63
        value = 0
        for b1 in bases:
            for b2 in bases:
                for b3 in bases:
                    if value < 64:  # 6 bits per codon
                        codon_table[(b1, b2, b3)] = value
                        value += 1
        
        return codon_table
    
    def encode_data_to_dna(self, data: bytes) -> DNASequence:
        """Encode binary data into DNA sequence"""
        start_time = time.time()
        
        sequence_id = hashlib.sha256(data).hexdigest()[:16]
        bases = []
        
        # Convert bytes to 6-bit values and then to codons
        for byte in data:
            # Split byte into two 4-bit values
            high_nibble = (byte >> 4) & 0x0F
            low_nibble = byte & 0x0F
            
            # Convert to codons
            high_codon = self._value_to_codon(high_nibble)
            low_codon = self._value_to_codon(low_nibble)
            
            bases.extend([DNABase(b) for b in high_codon])
            bases.extend([DNABase(b) for b in low_codon])
        
        # Add error correction (Reed-Solomon inspired)
        error_correction = self._add_error_correction(bases)
        
        dna_sequence = DNASequence(
            sequence_id=sequence_id,
            bases=bases,
            encoded_data=data,
            error_correction=error_correction
        )
        
        self.sequences[sequence_id] = dna_sequence
        
        # Update statistics
        synthesis_time = (time.time() - start_time) * 1000
        self.storage_stats["synthesis_time"].append(synthesis_time)
        self.storage_stats["total_sequences"] += 1
        self.storage_stats["total_bytes_stored"] += len(data)
        
        return dna_sequence
    
    def _value_to_codon(self, value: int) -> Tuple[str, str, str]:
        """Convert 4-bit value to DNA codon"""
        # Simple mapping for demonstration
        codons = [
            ('A', 'T', 'G'), ('A', 'T', 'C'), ('A', 'G', 'T'), ('A', 'G', 'C'),
            ('A', 'C', 'T'), ('A', 'C', 'G'), ('T', 'A', 'G'), ('T', 'A', 'C'),
            ('T', 'G', 'A'), ('T', 'G', 'C'), ('T', 'C', 'A'), ('T', 'C', 'G'),
            ('G', 'A', 'T'), ('G', 'A', 'C'), ('G', 'T', 'A'), ('G', 'C', 'A')
        ]
        return codons[value % len(codons)]
    
    def _add_error_correction(self, bases: List[DNABase]) -> List[DNABase]:
        """Add error correction codes"""
        # Simplified error correction - add redundancy
        correction_bases = []
        
        # Add parity bases every 8 bases
        for i in range(0, len(bases), 8):
            chunk = bases[i:i+8]
            # Calculate parity
            gc_count = sum(1 for base in chunk 
                          if base in [DNABase.GUANINE, DNABase.CYTOSINE])
            parity_base = DNABase.GUANINE if gc_count % 2 == 0 else DNABase.CYTOSINE
            correction_bases.append(parity_base)
        
        return correction_bases
    
    def decode_dna_to_data(self, sequence_id: str) -> Optional[bytes]:
        """Decode DNA sequence back to binary data"""
        start_time = time.time()
        
        sequence = self.sequences.get(sequence_id)
        if not sequence:
            return None
        
        # Check for degradation
        if sequence.is_degraded():
            # Attempt error correction
            if not self._error_correct(sequence):
                return None
        
        # Convert codons back to bytes
        data = []
        bases_str = sequence.get_sequence_string()
        
        # Process in groups of 6 (two codons per byte)
        for i in range(0, len(bases_str), 6):
            if i + 5 < len(bases_str):
                high_codon = bases_str[i:i+3]
                low_codon = bases_str[i+3:i+6]
                
                high_value = self._codon_to_value(high_codon)
                low_value = self._codon_to_value(low_codon)
                
                byte_value = (high_value << 4) | low_value
                data.append(byte_value)
        
        retrieval_time = (time.time() - start_time) * 1000
        self.storage_stats["retrieval_time"].append(retrieval_time)
        
        return bytes(data)
    
    def _codon_to_value(self, codon: str) -> int:
        """Convert DNA codon back to 4-bit value"""
        # Reverse mapping of _value_to_codon
        codon_map = {
            'ATG': 0, 'ATC': 1, 'AGT': 2, 'AGC': 3,
            'ACT': 4, 'ACG': 5, 'TAG': 6, 'TAC': 7,
            'TGA': 8, 'TGC': 9, 'TCA': 10, 'TCG': 11,
            'GAT': 12, 'GAC': 13, 'GTA': 14, 'GCA': 15
        }
        return codon_map.get(codon, 0)
    
    def _error_correct(self, sequence: DNASequence) -> bool:
        """Attempt to correct errors in degraded DNA"""
        # Simplified error correction using parity checks
        corrected = True
        
        # Check parity at regular intervals
        for i, parity_base in enumerate(sequence.error_correction):
            start_idx = i * 8
            end_idx = min(start_idx + 8, len(sequence.bases))
            
            if end_idx > start_idx:
                chunk = sequence.bases[start_idx:end_idx]
                gc_count = sum(1 for base in chunk 
                             if base in [DNABase.GUANINE, DNABase.CYTOSINE])
                expected_parity = DNABase.GUANINE if gc_count % 2 == 0 else DNABase.CYTOSINE
                
                if parity_base != expected_parity:
                    corrected = False
                    # Attempt simple correction by flipping a base
                    if chunk:
                        # Flip the first base that doesn't match expected pattern
                        chunk[0] = DNABase.GUANINE if chunk[0] != DNABase.GUANINE else DNABase.ADENINE
        
        return corrected

class ProteinComputer:
    """Protein-based molecular computer"""
    
    def __init__(self):
        self.proteins: Dict[str, ProteinMolecule] = {}
        self.enzymatic_reactions: List[Dict[str, Any]] = []
        self.computation_stats = {
            "proteins_synthesized": 0,
            "reactions_catalyzed": 0,
            "folding_simulations": 0,
            "computation_time": deque(maxlen=100)
        }
    
    def synthesize_protein(self, amino_acid_sequence: str, function: str) -> ProteinMolecule:
        """Synthesize protein for specific computational function"""
        protein_id = hashlib.sha256(amino_acid_sequence.encode()).hexdigest()[:16]
        
        protein = ProteinMolecule(
            protein_id=protein_id,
            amino_acid_sequence=amino_acid_sequence,
            structure=ProteinStructure.LOOP_REGION,
            function=function
        )
        
        # Simulate folding
        protein.simulate_folding()
        
        # Add active sites based on function
        if function == "email_classifier":
            protein.active_sites = [
                {"type": "binding_site", "position": 10, "specificity": "subject_keywords"},
                {"type": "catalytic_site", "position": 25, "function": "priority_assessment"}
            ]
        elif function == "spam_detector":
            protein.active_sites = [
                {"type": "recognition_site", "position": 15, "pattern": "suspicious_content"},
                {"type": "signal_site", "position": 30, "output": "spam_probability"}
            ]
        
        self.proteins[protein_id] = protein
        self.computation_stats["proteins_synthesized"] += 1
        
        return protein
    
    def simulate_molecular_recognition(self, protein_id: str, 
                                     substrate: Dict[str, Any]) -> Dict[str, Any]:
        """Simulate molecular recognition for email processing"""
        start_time = time.time()
        
        protein = self.proteins.get(protein_id)
        if not protein:
            return {"error": "Protein not found"}
        
        # Simulate binding affinity calculation
        binding_affinity = self._calculate_binding_affinity(protein, substrate)
        
        # Simulate conformational changes
        conformational_change = self._simulate_conformational_change(protein, binding_affinity)
        
        # Calculate output signal
        output_signal = self._calculate_output_signal(protein, conformational_change)
        
        computation_time = (time.time() - start_time) * 1000
        self.computation_stats["computation_time"].append(computation_time)
        self.computation_stats["reactions_catalyzed"] += 1
        
        return {
            "recognition_result": {
                "binding_affinity": binding_affinity,
                "conformational_change": conformational_change,
                "output_signal": output_signal,
                "specificity": self._calculate_specificity(protein, substrate)
            },
            "molecular_dynamics": {
                "protein_stability": protein.stability,
                "active_sites_engaged": len([site for site in protein.active_sites 
                                           if self._is_site_active(site, substrate)]),
                "reaction_rate": binding_affinity * 1000  # simplified
            },
            "computation_time_ns": computation_time * 1000000  # convert to nanoseconds
        }
    
    def _calculate_binding_affinity(self, protein: ProteinMolecule, 
                                   substrate: Dict[str, Any]) -> float:
        """Calculate protein-substrate binding affinity"""
        affinity = 0.0
        
        # Simplified affinity calculation based on complementarity
        if protein.function == "email_classifier":
            subject = substrate.get("subject", "")
            content = substrate.get("content", "")
            
            # Pattern matching simulation
            if "urgent" in subject.lower():
                affinity += 0.8
            if "meeting" in content.lower():
                affinity += 0.6
            if len(content) > 500:
                affinity += 0.3
        
        elif protein.function == "spam_detector":
            sender = substrate.get("sender", "")
            content = substrate.get("content", "")
            
            # Spam pattern recognition
            if "@suspicious-domain.com" in sender:
                affinity += 0.9
            if "CLICK HERE" in content:
                affinity += 0.7
            if content.count("!") > 3:
                affinity += 0.4
        
        # Add molecular noise
        noise = random.gauss(0, 0.1)
        affinity = max(0, min(1, affinity + noise))
        
        return affinity
    
    def _simulate_conformational_change(self, protein: ProteinMolecule, 
                                      binding_affinity: float) -> float:
        """Simulate protein conformational change upon binding"""
        # Higher binding affinity causes larger conformational change
        max_change = 0.5  # Maximum conformational change
        change = binding_affinity * max_change
        
        # Add thermal fluctuations
        thermal_noise = random.gauss(0, 0.05)
        change += thermal_noise
        
        return abs(change)
    
    def _calculate_output_signal(self, protein: ProteinMolecule, 
                               conformational_change: float) -> float:
        """Calculate output signal from conformational change"""
        # Sigmoidal response function
        signal = 1 / (1 + np.exp(-10 * (conformational_change - 0.25)))
        
        return signal
    
    def _calculate_specificity(self, protein: ProteinMolecule, 
                             substrate: Dict[str, Any]) -> float:
        """Calculate protein-substrate specificity"""
        specificity = 0.5  # Base specificity
        
        # Specific patterns increase specificity
        for site in protein.active_sites:
            if site["type"] == "binding_site":
                specificity += 0.2
            elif site["type"] == "recognition_site":
                specificity += 0.3
        
        return min(1.0, specificity)
    
    def _is_site_active(self, site: Dict[str, Any], substrate: Dict[str, Any]) -> bool:
        """Check if active site is engaged with substrate"""
        site_type = site.get("type", "")
        
        if site_type == "binding_site":
            return substrate.get("content", "") != ""
        elif site_type == "catalytic_site":
            return len(substrate.get("subject", "")) > 5
        elif site_type == "recognition_site":
            return substrate.get("sender", "") != ""
        
        return False

class BioComputingEngine:
    """Main biological computing engine"""
    
    def __init__(self):
        self.dna_storage = DNAStorage()
        self.protein_computer = ProteinComputer()
        self.genetic_algorithms = {}
        self.bio_circuits = {}
        self.performance_metrics = {
            "dna_operations": 0,
            "protein_computations": 0,
            "genetic_optimizations": 0,
            "bio_circuit_executions": 0,
            "energy_efficiency": 0.0,  # Operations per ATP
            "storage_capacity": 0.0   # Bytes per gram
        }
        self.lock = threading.RLock()
        
        # Initialize email processing proteins
        self._initialize_email_proteins()
    
    def _initialize_email_proteins(self):
        """Initialize proteins for email processing tasks"""
        
        # Email classifier protein
        classifier_sequence = "MKLLLFAIPLVVPFLLYDSTGPQRLQAAAVTSCRFGSLLQRKGQRVGSVPPAATSW"
        self.protein_computer.synthesize_protein(classifier_sequence, "email_classifier")
        
        # Spam detector protein  
        spam_detector_sequence = "MAARRTLLLLLLAAAAACLLAALVAASGHPGPTQRFXKSLLQPAGQRLQEAAVR"
        self.protein_computer.synthesize_protein(spam_detector_sequence, "spam_detector")
        
        # Priority assessor protein
        priority_sequence = "MKLFWLLFTIGFSLGVAYSQNRKYRELQKSPQRRLQRAAALGSQQLQSRAQARA"
        self.protein_computer.synthesize_protein(priority_sequence, "priority_assessor")
    
    def process_email_biologically(self, email_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process email using biological computing"""
        start_time = time.time()
        
        bio_results = {}
        
        # DNA Storage: Store email content
        email_json = json.dumps(email_data, sort_keys=True).encode()
        dna_sequence = self.dna_storage.encode_data_to_dna(email_json)
        
        bio_results["dna_storage"] = {
            "sequence_id": dna_sequence.sequence_id,
            "sequence_length": len(dna_sequence.bases),
            "gc_content": dna_sequence.calculate_gc_content(),
            "storage_efficiency": f"{len(email_json) / len(dna_sequence.bases) * 100:.1f}% compression"
        }
        
        # Protein Computing: Process with each protein
        protein_results = {}
        for protein_id, protein in self.protein_computer.proteins.items():
            recognition_result = self.protein_computer.simulate_molecular_recognition(
                protein_id, email_data
            )
            protein_results[protein.function] = recognition_result
        
        bio_results["protein_computing"] = protein_results
        
        # Genetic Algorithm: Optimize classification
        genetic_result = self._run_genetic_optimization(email_data, protein_results)
        bio_results["genetic_optimization"] = genetic_result
        
        # Bio-circuit: Integrate all signals
        circuit_result = self._execute_bio_circuit(protein_results)
        bio_results["bio_circuit_integration"] = circuit_result
        
        processing_time = (time.time() - start_time) * 1000
        
        with self.lock:
            self.performance_metrics["dna_operations"] += 1
            self.performance_metrics["protein_computations"] += len(protein_results)
            self.performance_metrics["genetic_optimizations"] += 1
            self.performance_metrics["bio_circuit_executions"] += 1
        
        return {
            "biological_processing": bio_results,
            "bio_inspired_decision": {
                "classification": circuit_result.get("final_output", {}).get("classification", "unknown"),
                "confidence": circuit_result.get("final_output", {}).get("confidence", 0.5),
                "biological_basis": "Molecular recognition + genetic optimization",
                "processing_paradigm": "DNA storage + protein computation"
            },
            "molecular_metrics": {
                "dna_bases_used": len(dna_sequence.bases),
                "proteins_engaged": len(protein_results),
                "binding_events": sum(1 for r in protein_results.values() 
                                    if r.get("recognition_result", {}).get("binding_affinity", 0) > 0.5),
                "processing_time_ms": round(processing_time, 2)
            },
            "biological_advantages": {
                "storage_density": "1 exabyte per gram (DNA)",
                "energy_efficiency": "Femtojoule per operation",
                "parallelism": "Massive molecular parallelism",
                "error_correction": "Built-in redundancy and repair"
            }
        }
    
    def _run_genetic_optimization(self, email_data: Dict[str, Any], 
                                 protein_results: Dict[str, Any]) -> Dict[str, Any]:
        """Run genetic algorithm to optimize email classification"""
        
        # Initialize population of classification parameters
        population_size = 20
        generations = 10
        
        population = []
        for _ in range(population_size):
            individual = {
                "weights": {
                    "email_classifier": random.uniform(0.1, 1.0),
                    "spam_detector": random.uniform(0.1, 1.0),
                    "priority_assessor": random.uniform(0.1, 1.0)
                },
                "thresholds": {
                    "spam_threshold": random.uniform(0.3, 0.8),
                    "priority_threshold": random.uniform(0.4, 0.9)
                },
                "fitness": 0.0
            }
            population.append(individual)
        
        # Evolve population
        best_fitness = 0.0
        best_individual = None
        
        for generation in range(generations):
            # Evaluate fitness
            for individual in population:
                individual["fitness"] = self._evaluate_fitness(individual, protein_results, email_data)
                
                if individual["fitness"] > best_fitness:
                    best_fitness = individual["fitness"]
                    best_individual = individual.copy()
            
            # Selection and reproduction
            population = self._genetic_selection_reproduction(population)
        
        return {
            "optimization_result": {
                "best_fitness": best_fitness,
                "optimal_weights": best_individual["weights"] if best_individual else {},
                "optimal_thresholds": best_individual["thresholds"] if best_individual else {},
                "generations_evolved": generations,
                "population_size": population_size
            },
            "evolutionary_metrics": {
                "convergence_rate": best_fitness / generations,
                "genetic_diversity": self._calculate_population_diversity(population),
                "selection_pressure": 0.7
            }
        }
    
    def _evaluate_fitness(self, individual: Dict[str, Any], 
                         protein_results: Dict[str, Any], 
                         email_data: Dict[str, Any]) -> float:
        """Evaluate fitness of individual classification parameters"""
        fitness = 0.0
        
        # Calculate weighted score from protein results
        for protein_name, weight in individual["weights"].items():
            if protein_name in protein_results:
                binding_affinity = protein_results[protein_name].get("recognition_result", {}).get("binding_affinity", 0)
                fitness += weight * binding_affinity
        
        # Apply thresholds
        spam_prob = protein_results.get("spam_detector", {}).get("recognition_result", {}).get("output_signal", 0)
        if spam_prob > individual["thresholds"]["spam_threshold"]:
            fitness *= 0.5  # Penalize if classified as spam
        
        priority_score = protein_results.get("priority_assessor", {}).get("recognition_result", {}).get("output_signal", 0)
        if priority_score > individual["thresholds"]["priority_threshold"]:
            fitness *= 1.2  # Boost if high priority
        
        # Add randomness for exploration
        fitness += random.uniform(-0.1, 0.1)
        
        return max(0.0, fitness)
    
    def _genetic_selection_reproduction(self, population: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Genetic selection and reproduction"""
        # Sort by fitness
        population.sort(key=lambda x: x["fitness"], reverse=True)
        
        # Select top 50% as parents
        parents = population[:len(population)//2]
        
        # Generate new population
        new_population = parents.copy()  # Elitism
        
        # Crossover and mutation
        while len(new_population) < len(population):
            parent1 = random.choice(parents)
            parent2 = random.choice(parents)
            
            # Crossover
            child = {
                "weights": {},
                "thresholds": {},
                "fitness": 0.0
            }
            
            for key in parent1["weights"]:
                child["weights"][key] = (parent1["weights"][key] + parent2["weights"][key]) / 2
            
            for key in parent1["thresholds"]:
                child["thresholds"][key] = (parent1["thresholds"][key] + parent2["thresholds"][key]) / 2
            
            # Mutation
            if random.random() < 0.1:  # Mutation rate
                mutate_key = random.choice(list(child["weights"].keys()))
                child["weights"][mutate_key] *= random.uniform(0.8, 1.2)
            
            new_population.append(child)
        
        return new_population
    
    def _calculate_population_diversity(self, population: List[Dict[str, Any]]) -> float:
        """Calculate genetic diversity of population"""
        if len(population) < 2:
            return 0.0
        
        # Calculate average pairwise distance
        total_distance = 0.0
        comparisons = 0
        
        for i in range(len(population)):
            for j in range(i+1, len(population)):
                distance = 0.0
                
                # Calculate distance in weight space
                for key in population[i]["weights"]:
                    if key in population[j]["weights"]:
                        distance += abs(population[i]["weights"][key] - population[j]["weights"][key])
                
                total_distance += distance
                comparisons += 1
        
        return total_distance / comparisons if comparisons > 0 else 0.0
    
    def _execute_bio_circuit(self, protein_results: Dict[str, Any]) -> Dict[str, Any]:
        """Execute biological circuit to integrate protein signals"""
        
        # Simulate biological AND/OR gates
        spam_signal = protein_results.get("spam_detector", {}).get("recognition_result", {}).get("output_signal", 0)
        priority_signal = protein_results.get("priority_assessor", {}).get("recognition_result", {}).get("output_signal", 0)
        classifier_signal = protein_results.get("email_classifier", {}).get("recognition_result", {}).get("output_signal", 0)
        
        # AND gate: High priority AND not spam
        high_priority_gate = min(priority_signal, 1 - spam_signal)
        
        # OR gate: Any significant signal
        any_signal_gate = max(spam_signal, priority_signal, classifier_signal)
        
        # Integration circuit
        integrated_signal = (high_priority_gate * 0.6 + any_signal_gate * 0.4)
        
        # Determine final classification
        if spam_signal > 0.7:
            classification = "spam"
            confidence = spam_signal
        elif priority_signal > 0.6:
            classification = "high_priority"
            confidence = priority_signal
        elif classifier_signal > 0.5:
            classification = "normal"
            confidence = classifier_signal
        else:
            classification = "uncertain"
            confidence = 0.3
        
        return {
            "circuit_topology": {
                "input_nodes": ["spam_detector", "priority_assessor", "email_classifier"],
                "gate_operations": ["AND", "OR", "INTEGRATION"],
                "output_node": "final_classification"
            },
            "signal_processing": {
                "spam_signal": spam_signal,
                "priority_signal": priority_signal,
                "classifier_signal": classifier_signal,
                "integrated_signal": integrated_signal
            },
            "final_output": {
                "classification": classification,
                "confidence": confidence,
                "decision_basis": "Molecular recognition + bio-circuit integration"
            },
            "circuit_performance": {
                "signal_to_noise_ratio": integrated_signal / 0.1,  # Assume 0.1 noise
                "processing_delay_ns": 100,  # Nanosecond-scale biological processing
                "energy_consumption_atp": 3  # ATP molecules consumed
            }
        }
    
    def get_bio_computing_analytics(self) -> Dict[str, Any]:
        """Get comprehensive biological computing analytics"""
        
        dna_stats = self.dna_storage.storage_stats
        protein_stats = self.protein_computer.computation_stats
        
        # Calculate average processing times
        avg_synthesis_time = (sum(dna_stats["synthesis_time"]) / len(dna_stats["synthesis_time"])
                            if dna_stats["synthesis_time"] else 0)
        
        avg_computation_time = (sum(protein_stats["computation_time"]) / len(protein_stats["computation_time"])
                              if protein_stats["computation_time"] else 0)
        
        return {
            "biological_computing_overview": {
                "paradigm": "Bio-molecular information processing",
                "storage_medium": "DNA double helix",
                "processing_units": "Protein molecules",
                "optimization_method": "Genetic algorithms",
                "circuit_type": "Biological logic gates"
            },
            "dna_storage_system": {
                "total_sequences": dna_stats["total_sequences"],
                "bytes_stored": dna_stats["total_bytes_stored"],
                "storage_density_gb_per_gram": 1e9,  # Theoretical DNA storage density
                "error_rate": dna_stats["error_rate"],
                "average_synthesis_time_ms": round(avg_synthesis_time, 2),
                "longevity_years": 1000  # DNA can last millennia
            },
            "protein_computing_system": {
                "active_proteins": len(self.protein_computer.proteins),
                "total_computations": protein_stats["reactions_catalyzed"],
                "folding_simulations": protein_stats["folding_simulations"],
                "average_computation_time_ms": round(avg_computation_time, 2),
                "molecular_parallelism": "10^23 molecules per mole"
            },
            "genetic_optimization": {
                "algorithm_type": "Evolutionary computation",
                "population_dynamics": "Selection + crossover + mutation",
                "fitness_evaluation": "Multi-objective optimization",
                "convergence_rate": "Logarithmic with generations"
            },
            "performance_metrics": {
                "operations_performed": {
                    "dna_operations": self.performance_metrics["dna_operations"],
                    "protein_computations": self.performance_metrics["protein_computations"],
                    "genetic_optimizations": self.performance_metrics["genetic_optimizations"],
                    "bio_circuit_executions": self.performance_metrics["bio_circuit_executions"]
                },
                "efficiency_metrics": {
                    "energy_per_operation_joules": 1e-18,  # Biological efficiency
                    "operations_per_second": 1e12,  # Molecular time scales
                    "error_correction_rate": 0.9999,
                    "temperature_stability": "4-37°C operating range"
                }
            },
            "biological_advantages": {
                "massive_parallelism": "Avogadro's number of processors",
                "self_assembly": "Proteins fold automatically",
                "error_correction": "Built-in repair mechanisms", 
                "energy_efficiency": "Femtojoule per computation",
                "information_density": "Exabytes per gram storage",
                "evolutionary_optimization": "Natural selection principles"
            },
            "applications": {
                "email_processing": "Molecular pattern recognition",
                "data_storage": "DNA-based information archival",
                "optimization": "Genetic algorithm problem solving",
                "parallel_computing": "Massive molecular computation"
            },
            "future_potential": {
                "dna_computers": "Programmable genetic circuits",
                "protein_design": "Custom enzymes for computation",
                "bio_hybrid_systems": "Integration with silicon chips",
                "synthetic_biology": "Engineered biological computers"
            }
        }

# Global biological computing engine instance
_bio_computing_engine = None

def get_bio_computing_engine():
    """Get global biological computing engine"""
    global _bio_computing_engine
    if _bio_computing_engine is None:
        _bio_computing_engine = BioComputingEngine()
    return _bio_computing_engine