# Workload

- YCSB
  - YCSB-A
  - YCSB-B
  - YCSB-C
  - YCSB-D
  - YCSB-E
  - YCSB-F
- TPCC
  - TPCC_Light: TPCC without insert and delete
  - TPCC

# Interface

## Type Interface
- **Workload**
  - `Workload::KeyType`: The type of key in this workload
  - `Workload::AbKeyType`: The abstract key of this workload, which indicates the type of the table and the key.
  - `Workload::Transaction`: The type of transaction
  - `Workload::TableSchemeSizeDefinition`: A initializer_list of size of each table 

## Function Interface

- **Workload**
  - `std::vector<Transaction> initialize_insert()`: 
  Get a list of transaction for initialization, which mainly includes insert
  - `Transaction generate_transaction()`:
  Generate a transaction randomly. 
  Note that initialization should have been accomplished when call this function.
- **Transaction**
  - `bool is_only_read()`: 
  Return bool variant indicating whether this transaction only includes read operation.
  - `bool run(ExecutorType &executor)`:
  Execute this transaction through specific executor, 
  **the concept of executor is defined as ` ExecutorConcept` in [abstract_transaction.h](include/workload/abstract_transaction.h)**



# Reference

- YSCB: https://github.com/brianfrankcooper/YCSB
- TPCC: https://github.com/evanj/tpccbench