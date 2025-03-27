import networkx as nx
import matplotlib.pyplot as plt
from avro.datafile import DataFileReader
from avro.io import DatumReader
import json

def read_pfb_files():
    patients = []
    diagnoses = []
    
    with DataFileReader(open("temp/output/patients.pfb", "rb"), DatumReader()) as reader:
        for patient in reader:
            patients.append(patient)
            
    with DataFileReader(open("temp/output/diagnoses.pfb", "rb"), DatumReader()) as reader:
        for diagnosis in reader:
            diagnoses.append(diagnosis)
            
    return patients, diagnoses

def draw_simple_graph():
    patients, diagnoses = read_pfb_files()
    
    plt.figure(figsize=(10, 6))
    
    patient_y = {}
    for i, patient in enumerate(patients):
        y_pos = (len(patients) - i) * 2
        patient_y[patient['id']] = y_pos
        
        plt.scatter(1, y_pos, s=500, color='skyblue', edgecolor='black', zorder=2)
        plt.text(1, y_pos, patient['name'], ha='center', va='center', fontsize=12)
    
    diag_y = {}
    for i, diagnosis in enumerate(diagnoses):
        y_pos = (len(diagnoses) - i + 0.5) * 2
        diag_y[diagnosis['id']] = y_pos
        
        plt.scatter(3, y_pos, s=500, color='lightgreen', edgecolor='black', zorder=2)
        plt.text(3, y_pos, f"{diagnosis['code']}\n{diagnosis['description']}", 
                ha='center', va='center', fontsize=10)
    
    for patient in patients:
        for diag in patient['diagnoses']:
            diag_id = diag['diagnosis_id']
            if diag_id in diag_y:
                plt.plot([1, 3], [patient_y[patient['id']], diag_y[diag_id]], 
                        'k-', zorder=1, alpha=0.7)
    
    # Add legend
    plt.scatter([], [], s=100, color='skyblue', edgecolor='black', label='Patient')
    plt.scatter([], [], s=100, color='lightgreen', edgecolor='black', label='Diagnosis')
    plt.legend()
    
    plt.title("Healthcare Graph: Patients and their Diagnoses")
    plt.xlim(0, 4)
    plt.axis('off')
    plt.tight_layout()
    plt.savefig("healthcare_graph_simple.png")
    plt.show()

if __name__ == "__main__":
    draw_simple_graph()