# Lineage-Based-Architecture-Database

In this program I along with my team of developers have created a real code implementation of the L-Store Database Architecture. The database acts as a hybrid, meaning it offers both transactional (OTLP) and analytical (OLAP) workloads with a single engine, ensuring a wider arrange of uses. A significant advantage of the database architecture is its speed with regard to querying due to the lineage approach. Additionally, L-Store's handling of locks reduces contention, ensuring data integrity and improving the systems throughput. The approach of lazy staging to disk in L-Store optimizes initial data handling in-memory, then efficiently persists data, balancing write and read performance. The strategy ensures a high performance for both writes and reads, making it ideal for handling complex workloads with minimal latency. Through several month long project, the program showcases the flexbility of the L-Store based Database Architecture and demonstrates its potential for storing data in the future. 



Test Cases:

1) A general Base Test that tests inserting, selecting, updating and deleting records. It also shows the aggregation of a product batch. The run times are also included for the performance.

2) The Transactional Test tests existing transactional operations including insertions and queries in a multi threaded environment. It tests the accuracy of data handling to ensure 100% accuracy.



References:

The database architecture L-Store, was developed based on the Industrial and Application paper "L-Store: A Real-time OLTP and OLAP System" by Mohammad Sadoghi, Souvik Bhattacherjee, Bishwaranjan Bhattacharjee and Mustafa Canim.









