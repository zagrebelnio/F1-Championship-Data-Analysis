import nbformat

def extract_code_from_ipynb(ipynb_file, output_file):
    with open(ipynb_file, 'r', encoding='utf-8') as file:
        notebook = nbformat.read(file, as_version=4)
        
    code_cells = [cell['source'] for cell in notebook['cells'] if cell['cell_type'] == 'code']
    
    with open(output_file, 'w', encoding='utf-8') as file:
        for i, code in enumerate(code_cells, 1):
            file.write(code)
            file.write('\n\n')

files = {
    'create_storage.py': '../storage/scripts/create_storage.ipynb',
    'dimension_tables.py': '../storage/scripts/dimension_tables.ipynb',
    'fact_tables.py': '../storage/scripts/fact_tables.ipynb',
    'lap_times.py': '../storage/scripts/lap_times.ipynb',
    'k-nearest_neighbours.py': '../prediction/k-nearest_neighbours.ipynb',
    'logistic-regression.py': '../prediction/logistic-regression.ipynb',
    'random_forest.py': '../prediction/random_forest.ipynb',
    'support_vector_machines.py': '../prediction/support_vector_machines.ipynb',
}

for py, ipynb in files.items():
    extract_code_from_ipynb(ipynb, py)
