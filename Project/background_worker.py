import psycopg2
from collections import Counter

host = "localhost"
port = "5432"
dbname = "test"
user = "ubuntu-decs"
password = "12345678"

def process_files(file1_path, file2_path):
    count_dict = Counter()
    with open(file1_path, 'r') as file1:
        for line in file1:
            line = line.strip()
            count_dict[line] += 1

    frequent_pairs = {pair for pair, count in count_dict.items() if count > 5}

    to_remove = set()
    with open(file2_path, 'r') as file2:
        for line in file2:
            to_remove.add(line.strip())

    frequent_pairs -= to_remove

    remaining_lines = []
    with open(file1_path, 'r') as file1:
        for line in file1:
            line = line.strip()
            if line not in frequent_pairs and line not in to_remove:
                remaining_lines.append(line)

    with open(file1_path, 'w') as file1:
        for line in remaining_lines:
            file1.write(line + '\n')

    with open(file2_path, 'a') as file2:
        for pair in frequent_pairs:
            file2.write(pair + '\n')

    return frequent_pairs


def create_indexes_from_pairs(frequent_pairs, cursor):

    for pair in frequent_pairs:
        table_name, column_name = pair.split(', ')
        
        index_name = f"{table_name}_{column_name}_idx"
        
        create_index_query = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({column_name});"
        
        try:
            print(f"Executing: {create_index_query}")
            cursor.execute(create_index_query)
        except Exception as e:
            print(f"Error creating index on {table_name}.{column_name}: {e}")


if __name__ == "__main__":

    stat_file = "/home/ubuntu-decs/Desktop/CS-SEM1/cs631/Project/stats.txt"
    index_file = "/home/ubuntu-decs/Desktop/CS-SEM1/cs631/Project/created_indices.txt"
    
    indices = process_files(stat_file, index_file)
    
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )

        cursor = conn.cursor()

        create_indexes_from_pairs(indices, cursor)

        conn.commit()

        sql_query = "SELECT * FROM takes;"
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        for row in rows:
            print(row)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
