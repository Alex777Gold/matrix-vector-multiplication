from pyspark import SparkContext, SparkConf


def matrix_vector_multiply(row):
    matrix, vector = row
    result = [sum(x * y for x, y in zip(matrix_row, vector))
              for matrix_row in matrix]
    return result


if __name__ == "__main__":
    conf = SparkConf().setAppName("MatrixVectorMultiply")
    sc = SparkContext(conf=conf)

    # Assuming the matrix is a square matrix of dimension N and the vector has dimension N
    matrix_size = int(input("Enter the dimension N for the square matrix: "))
    matrix = [[float(input(f"Enter element [{i}][{j}] of the matrix: ")) for j in range(
        matrix_size)] for i in range(matrix_size)]
    vector = [
        float(input(f"Enter element {i} of the vector: ")) for i in range(matrix_size)]

    # Broadcast the matrix and vector to all worker nodes
    broadcast_matrix = sc.broadcast(matrix)
    broadcast_vector = sc.broadcast(vector)

    # Create an RDD with the matrix and vector
    data = sc.parallelize([(broadcast_matrix.value, broadcast_vector.value)])

    # Perform matrix-vector multiplication using map
    result = data.map(matrix_vector_multiply).collect()

    # Print the result
    print("Result of Matrix-Vector Multiplication:")
    print(result)

    # Stop the SparkContext
    sc.stop()
