import json
import os
import time
import networkx as nx
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader
import avro.schema


def create_sample_clinical_graph():
    G = nx.DiGraph()
    
    G.add_node("patient-1", type="Patient", name="John Doe", age=45)
    G.add_node("patient-2", type="Patient", name="Jane Smith", age=62)
    G.add_node("diagnosis-1", type="Diagnosis", code="E11.9", description="Type 2 diabetes")
    G.add_node("diagnosis-2", type="Diagnosis", code="I10", description="Hypertension")
    
    G.add_edge("patient-1", "diagnosis-1", type="has_diagnosis", date="2023-10-12")
    G.add_edge("patient-2", "diagnosis-1", type="has_diagnosis", date="2022-05-30")
    G.add_edge("patient-2", "diagnosis-2", type="has_diagnosis", date="2021-11-14")
    
    return G


def create_schemas(output_dir):
    os.makedirs(output_dir, exist_ok=True)
    
    patient_schema = {
        "namespace": "healthcare",
        "type": "record",
        "name": "Patient",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"},
            {"name": "diagnoses", "type": {"type": "array", "items": {
                "type": "record",
                "name": "DiagnosisReference",
                "fields": [
                    {"name": "diagnosis_id", "type": "string"},
                    {"name": "date", "type": "string"}
                ]
            }}, "default": []}
        ]
    }
    
    diagnosis_schema = {
        "namespace": "healthcare",
        "type": "record",
        "name": "Diagnosis",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "code", "type": "string"},
            {"name": "description", "type": "string"},
            {"name": "patients", "type": {"type": "array", "items": "string"}, "default": []}
        ]
    }
    
    with open(f"{output_dir}/patient.avsc", "w") as f:
        json.dump(patient_schema, f, indent=2)
    
    with open(f"{output_dir}/diagnosis.avsc", "w") as f:
        json.dump(diagnosis_schema, f, indent=2)
    
    return {
        "Patient": avro.schema.parse(json.dumps(patient_schema)),
        "Diagnosis": avro.schema.parse(json.dumps(diagnosis_schema))
    }


def convert_graph_to_pfb(graph, schemas, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    
    patients = [(n, d) for n, d in graph.nodes(data=True) if d.get('type') == 'Patient']
    diagnoses = [(n, d) for n, d in graph.nodes(data=True) if d.get('type') == 'Diagnosis']
    
    with DataFileWriter(open(f"{output_dir}/patients.pfb", "wb"), DatumWriter(), schemas["Patient"]) as writer:
        for patient_id, patient_data in patients:
            patient_diagnoses = []
            for _, diagnosis_id, edge_data in graph.out_edges(patient_id, data=True):
                if edge_data.get('type') == 'has_diagnosis':
                    patient_diagnoses.append({
                        "diagnosis_id": diagnosis_id,
                        "date": edge_data.get('date', 'unknown')
                    })
            
            record = {
                "id": patient_id,
                "name": patient_data.get('name', ''),
                "age": patient_data.get('age', 0),
                "diagnoses": patient_diagnoses
            }
            writer.append(record)
    
    with DataFileWriter(open(f"{output_dir}/diagnoses.pfb", "wb"), DatumWriter(), schemas["Diagnosis"]) as writer:
        for diagnosis_id, diagnosis_data in diagnoses:
            patient_ids = [src for src, tgt, _ in graph.in_edges(diagnosis_id, data=True) if tgt == diagnosis_id]
            
            record = {
                "id": diagnosis_id,
                "code": diagnosis_data.get('code', ''),
                "description": diagnosis_data.get('description', ''),
                "patients": patient_ids
            }
            writer.append(record)
    
    return {
        "patients": f"{output_dir}/patients.pfb",
        "diagnoses": f"{output_dir}/diagnoses.pfb"
    }


def read_pfb_files(pfb_files, schemas):
    print("\n=== Reading PFB Files ===")
    
    print("\nPatients:")
    with DataFileReader(open(pfb_files["patients"], "rb"), DatumReader()) as reader:
        for patient in reader:
            print(f"  - {patient['name']} (Age: {patient['age']})")
            print(f"    Diagnoses: {len(patient['diagnoses'])}")
            for diag in patient['diagnoses']:
                print(f"      * {diag['diagnosis_id']} (Date: {diag['date']})")
    
    print("\nDiagnoses:")
    with DataFileReader(open(pfb_files["diagnoses"], "rb"), DatumReader()) as reader:
        for diagnosis in reader:
            print(f"  - {diagnosis['code']}: {diagnosis['description']}")
            print(f"    Patients: {len(diagnosis['patients'])}")
            for patient_id in diagnosis['patients']:
                print(f"      * {patient_id}")


def main():
    print("\n====================================")
    print("  Graph to PFB Conversion Demo      ")
    print("====================================\n")
    
    os.makedirs("temp", exist_ok=True)
    
    start_time = time.time()
    
    print("Step 1: Creating sample clinical graph...")
    graph = create_sample_clinical_graph()
    print(f"  Created graph with {graph.number_of_nodes()} nodes and {graph.number_of_edges()} edges")
    
    print("\nStep 2: Creating Avro schemas...")
    schemas = create_schemas("temp/schemas")
    print("  Created schemas for Patient and Diagnosis")
    
    print("\nStep 3: Converting graph to PFB format...")
    pfb_files = convert_graph_to_pfb(graph, schemas, "temp/output")
    print("  Conversion complete!")
    
    read_pfb_files(pfb_files, schemas)
    
    elapsed_time = time.time() - start_time
    print(f"\nDemo completed in {elapsed_time:.2f} seconds")


if __name__ == "__main__":
    main()