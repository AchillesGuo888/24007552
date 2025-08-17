import os
import yaml
import json
import pandas as pd
from glob import glob
from collections import defaultdict
import traceback

class MicroserviceMetricsCalculator:
    def __init__(self, config_path="config.yaml"):
        try:
            self.config = self.load_config(config_path)
            self.entities = self.load_entities()
            self.operations = self.load_operations()
            self.op_type_map = self.config.get("op_type_map", {})
            self.schemes = self.load_schemes()
            
            # Operation type weights
            self.op_weights = self.config.get("op_weights", {
                "Create": 4, "Update": 3, "Delete": 2, "Read": 1
            })
            print("Configuration loaded successfully!")
        except Exception as e:
            print(f"Initialization failed: {str(e)}")
            raise
    
    def load_config(self, path):
        """Load YAML configuration file"""
        if not os.path.exists(path):
            raise FileNotFoundError(f"Config file not found: {path}")
        
        with open(path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            
        # Validate required keys
        required_keys = ["entities_file", "operations_file", "schemes_dir"]
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required key in config: {key}")
        
        return config
    
    def load_entities(self):
        """Load entity definitions from JSON file"""
        entity_file = self.config["entities_file"]
        if not os.path.exists(entity_file):
            raise FileNotFoundError(f"Entity file not found: {entity_file}")
        
        print(f"Loading entity file: {entity_file}")
        
        try:
            # Attempt direct loading
            with open(entity_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except json.JSONDecodeError:
            # Try handling BOM if needed
            with open(entity_file, 'r', encoding='utf-8-sig') as f:
                data = json.load(f)
        
        return self._parse_entities(data)
    
    def _parse_entities(self, data):
        """Parse entity data"""
        entities = {}
        if "entities" not in data:
            raise ValueError("Missing 'entities' key in entity file")
        
        for entity in data["entities"]:
            if "name" not in entity or "nanoentities" not in entity:
                raise ValueError("Incomplete entity definition")
                
            full_name = entity["name"]
            entities[full_name] = [f"{full_name}.{attr}" for attr in entity["nanoentities"]]
        
        print(f"Loaded {len(entities)} entities")
        return entities
    
    def load_operations(self):
        """Load operation details from YAML file"""
        ops_file = self.config["operations_file"]
        if not os.path.exists(ops_file):
            raise FileNotFoundError(f"Operations file not found: {ops_file}")
        
        print(f"Loading operations file: {ops_file}")
        
        with open(ops_file, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        if "operations" not in data:
            raise ValueError("Missing 'operations' key in operations file")
        
        operations = {}
        for op in data["operations"]:
            if "name" not in op:
                continue
                
            op_name = op["name"]
            operations[op_name] = {"read": [], "write": []}
            
            for access in op.get("database_access", []):
                if "entity_name" not in access:
                    continue
                    
                entity = access["entity_name"]
                if "read_attributes" in access:
                    operations[op_name]["read"].extend(
                        [f"{entity}.{attr}" for attr in access["read_attributes"]]
                    )
                if "write_attributes" in access:
                    operations[op_name]["write"].extend(
                        [f"{entity}.{attr}" for attr in access["write_attributes"]]
                    )
        
        print(f"Loaded {len(operations)} operations")
        return operations
    
    def load_schemes(self):
        """Load all decomposition schemes"""
        scheme_dir = self.config["schemes_dir"]
        if not os.path.exists(scheme_dir):
            raise NotADirectoryError(f"Schemes directory not found: {scheme_dir}")
        
        print(f"Loading schemes directory: {scheme_dir}")
        
        schemes = {}
        json_files = glob(os.path.join(scheme_dir, "*.json"))
        
        if not json_files:
            print(f"Warning: No JSON files found in {scheme_dir}")
            return schemes
        
        for file_path in json_files:
            try:
                # Try UTF-8 encoding
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except UnicodeDecodeError:
                # Try handling BOM
                with open(file_path, 'r', encoding='utf-8-sig') as f:
                    data = json.load(f)
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON file {file_path}: {str(e)}")
                continue
            
            scheme_name = os.path.splitext(os.path.basename(file_path))[0]
            services = []
            
            # Parse service structure
            if "services" not in data:
                print(f"Warning: Missing 'services' key in {file_path}")
                continue
                
            for svc in data["services"]:
                if "name" not in svc:
                    continue
                    
                service_name = svc["name"]
                # Get use case list
                if "useCaseResponsibility" in data and service_name in data["useCaseResponsibility"]:
                    use_cases = data["useCaseResponsibility"][service_name]
                else:
                    use_cases = []
                services.append({"name": service_name, "use_cases": use_cases})
            
            schemes[scheme_name] = {"services": services}
            print(f"Loaded scheme: {scheme_name} ({len(services)} services)")
        
        return schemes
    
    def calculate_metrics(self):
        """Calculate metrics for all schemes"""
        all_results = []
        scheme_summary = []
        
        if not self.schemes:
            print("Warning: No decomposition schemes available")
            return pd.DataFrame(), pd.DataFrame()
        
        for scheme_name, scheme_data in self.schemes.items():
            try:
                print(f"Calculating metrics for scheme: {scheme_name}")
                scheme_results, scheme_metrics = self.calculate_scheme_metrics(
                    scheme_name, scheme_data["services"]
                )
                all_results.extend(scheme_results)
                scheme_summary.append({
                    "Scheme": scheme_name,
                    "ALCOM": scheme_metrics["ALCOM"],
                    "ASGM": scheme_metrics["ASGM"],
                    "NOO(max)": scheme_metrics["Max_NOO"]
                })
            except Exception as e:
                print(f"Error processing scheme {scheme_name}: {str(e)}")
                continue
        
        return pd.DataFrame(all_results), pd.DataFrame(scheme_summary)
    
    def calculate_scheme_metrics(self, scheme_name, services):
        """Calculate metrics for a single scheme"""
        if not services:
            raise ValueError("No services defined for this scheme")
            
        results = []
        scheme_metrics = {
            "ALCOM": 0, "ASGM": 0, "Max_NOO": 0
        }
        service_count = len(services)
        
        for service in services:
            # Get all use cases for the service
            use_cases = service["use_cases"]
            if not use_cases:
                print(f"Warning: Service {service['name']} has no use cases")
                continue
                
            noo = len(use_cases)
            scheme_metrics["Max_NOO"] = max(scheme_metrics["Max_NOO"], noo)
            
            # Collect all unique parameters for the service
            all_params = set()
            param_access_count = defaultdict(int)
            read_params = set()
            write_params = set()
            
            for uc in use_cases:
                if uc not in self.operations:
                    print(f"Warning: Operation '{uc}' definition not found")
                    continue
                    
                for param in self.operations[uc]["read"]:
                    all_params.add(param)
                    param_access_count[param] += 1
                    read_params.add(param)
                for param in self.operations[uc]["write"]:
                    all_params.add(param)
                    param_access_count[param] += 1
                    write_params.add(param)
            
            # Calculate LCOM
            mf_sum = sum(param_access_count.values())
            m = noo
            f = len(all_params)
            lcom = 1 - (mf_sum / (m * f)) if m * f > 0 else 0
            
            # Calculate DGS and FGS for each operation
            service_weight = 0
            operation_details = []
            
            for uc in use_cases:
                if uc not in self.operations:
                    continue
                    
                # Get operation type (default to Read)
                op_type = self.op_type_map.get(uc, "Read")
                ot = self.op_weights.get(op_type, 1)
                service_weight += ot
                
                # Calculate DGS
                uc_read = set(self.operations[uc]["read"])
                uc_write = set(self.operations[uc]["write"])
                ipr = len(uc_read)
                opr = len(uc_write)
                fp = len(read_params) if read_params else 1
                cp = len(write_params) if write_params else 1
                dgs = min(1.0, (ipr/max(1, fp) + opr/max(1, cp)))
                
                operation_details.append({
                    "Operation": uc,
                    "IPR": ipr,
                    "OPR": opr,
                    "FP": fp,
                    "CP": cp,
                    "OT": ot,
                    "DGS": dgs
                })
            
            # Calculate FGS and SGM
            sgm = 0
            for op in operation_details:
                fgs = op["OT"] / service_weight if service_weight > 0 else 0
                op["FGS"] = fgs
                op["SGM_Operation"] = op["DGS"] * fgs
                sgm += op["SGM_Operation"]
            
            # Add to results
            for op in operation_details:
                results.append({
                    "Scheme": scheme_name,
                    "Service": service["name"],
                    "NOO": noo,
                    "LCOM": lcom,
                    "SGM": sgm,
                    "MF": mf_sum,
                    "M": m,
                    "F": f,
                    "Operation": op["Operation"],
                    "IPR": op["IPR"],
                    "OPR": op["OPR"],
                    "FP": op["FP"],
                    "CP": op["CP"],
                    "OT": op["OT"],
                    "O": service_weight,
                    "DGS": op["DGS"],
                    "FGS": op["FGS"],
                    "SGM_Operation": op["SGM_Operation"]
                })
            
            # Update scheme-level metrics
            scheme_metrics["ALCOM"] += lcom
            scheme_metrics["ASGM"] += sgm
        
        # Calculate averages
        if service_count > 0:
            scheme_metrics["ALCOM"] /= service_count
            scheme_metrics["ASGM"] /= service_count
        
        return results, scheme_metrics
    
    def export_to_excel(self, output_file="microservices_metrics.xlsx"):
        """Export results to Excel"""
        try:
            detailed_df, summary_df = self.calculate_metrics()
            
            if detailed_df.empty or summary_df.empty:
                print("Warning: No data to export")
                return None, None
            
            # Ensure output directory exists
            output_dir = os.path.dirname(output_file) or "."
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
                print(f"Created output directory: {output_dir}")
            
            # Generate two Excel files
            base_name, ext = os.path.splitext(output_file)
            detailed_output = f"{base_name}_detailed{ext}"
            summary_output = f"{base_name}_summary{ext}"
            
            # Write detailed analysis file
            with pd.ExcelWriter(detailed_output) as writer:
                detailed_df.to_excel(writer, sheet_name='Detailed Analysis', index=False)
            print(f"Detailed analysis exported to {detailed_output}")
            
            # Write scheme summary file
            with pd.ExcelWriter(summary_output) as writer:
                summary_df.to_excel(writer, sheet_name='Scheme Summary', index=False)
            print(f"Scheme summary exported to {summary_output}")
            
            return detailed_output, summary_output
        except Exception as e:
            print(f"Excel export failed: {str(e)}")
            return None, None


def main():
    print("Starting microservice metrics calculation...")
    try:
        # Set current working directory to script directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(script_dir)
        print(f"Current working directory: {os.getcwd()}")
        
        # Config file path
        config_path = "config.yaml"
        print(f"Using config file: {config_path}")
        
        # Create calculator instance
        calculator = MicroserviceMetricsCalculator(config_path)
        
        # Set output file path
        output_file = "results/microservices_metrics.xlsx"
        detailed_path, summary_path = calculator.export_to_excel(output_file)
        
        if detailed_path and summary_path:
            print(f"Detailed report: {detailed_path}")
            print(f"Summary report: {summary_path}")
        else:
            print("Report generation failed")
    except Exception as e:
        print(f"Execution failed: {str(e)}")
        print(traceback.format_exc())
        print("Please check configuration and data formats")


if __name__ == "__main__":
    main()