import os
#import zip

#Dynamic Path
directory = os.path.dirname(os.path.abspath(__file__))
splitted = directory.split('\\')
base=''
for x in range(len(splitted)):
    base += splitted[x]
    base += '/'




def split_csv(input_file, output_directory, chunk_size):
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    with open(input_file, 'r') as f:
        header = f.readline()
        chunk = [header]
        chunk_size_bytes = len(header)

        for line in f:
            chunk.append(line)
            chunk_size_bytes += len(line)

            if chunk_size_bytes >= chunk_size * 1024 * 1024:
                output_file = os.path.join(output_directory, f'chunk_{len(os.listdir(output_directory)) + 1}.csv')
                with open(output_file, 'w') as chunk_file:
                    chunk_file.writelines(chunk)
                chunk = [header]
                chunk_size_bytes = len(header)

        if len(chunk) > 1:
            output_file = os.path.join(output_directory, f'chunk_{len(os.listdir(output_directory)) + 1}.csv')
            with open(output_file, 'w') as chunk_file:
                chunk_file.writelines(chunk)

if __name__ == '__main__':
    big_csv = base + 'data/annual-enterprise-survey-2021-financial-year-provisional-csv.csv'  # Replace with your input CSV file path
    output_folder = base + 'output'  # Replace with the desired output directory
    chunk_size_mb = 2

    split_csv(big_csv, output_folder, chunk_size_mb)
    print('CSV file split complete.')