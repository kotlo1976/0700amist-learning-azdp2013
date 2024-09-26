# 1. Aggregate
Scenario: Calculate average salary by department.

Input:
- Employee Table:
  ```
  | EmployeeID | DepartmentID | Salary |
  |------------|--------------|--------|
  | 1          | 1            | 60000  |
  | 2          | 1            | 80000  |
  | 3          | 2            | 70000  |
  | 4          | 2            | 90000  |
  ```

Transformation: 
- Group by `DepartmentID` and compute the average salary.

Output:
```
| DepartmentID | AverageSalary |
|--------------|---------------|
| 1            | 70000         |
| 2            | 80000         |
```

# 2. Alter Row
Scenario: Upsert employee records.

Input:
- Existing Employee Records:
  ```
  | EmployeeID | Name   | Salary |
  |------------|--------|--------|
  | 1          | John   | 60000  |
  ```

Transformation:
- If `EmployeeID = 1`, update `Salary` to 65000. If not present, insert new record.

Output:
```
| EmployeeID | Name   | Salary |
|------------|--------|--------|
| 1          | John   | 65000  |
```

# 3. Assert
Scenario: Ensure no employee has a duplicate ID.

Input:
- Employee Records:
  ```
  | EmployeeID | Name   | Salary |
  |------------|--------|--------|
  | 1          | John   | 60000  |
  | 1          | Jane   | 70000  |  // Duplicate
  ```

Transformation: Assert that `EmployeeID` must be unique.

Output: 
- Error for the duplicate entry.

# 4. Cast
Scenario: Convert salary data type from string to integer.

Input:
- Employee Records:
  ```
  | EmployeeID | Name   | Salary  |
  |------------|--------|---------|
  | 1          | John   | "60000" |
  ```

Transformation: Cast `Salary` to integer.

Output:
```
| EmployeeID | Name   | Salary |
|------------|--------|--------|
| 1          | John   | 60000  |
```

# 5. Conditional Split
Scenario: Split employees based on salary.

Input:
```
| EmployeeID | Name   | Salary |
|------------|--------|--------|
| 1          | John   | 60000  |
| 2          | Jane   | 50000  |
| 3          | Mark   | 80000  |
```

Transformation: Create two streams: `HighSalary` (Salary > 55000) and `LowSalary`.

Output:
- HighSalary:
  ```
  | EmployeeID | Name | Salary |
  |------------|------|--------|
  | 1          | John | 60000  |
  | 3          | Mark | 80000  |
  ```

- LowSalary:
  ```
  | EmployeeID | Name | Salary |
  |------------|------|--------|
  | 2          | Jane | 50000  |
  ```

# 6. Derived Column
Scenario: Add a tax column based on salary.

Input:
```
| EmployeeID | Name   | Salary |
|------------|--------|--------|
| 1          | John   | 60000  |
```

Transformation: Add `Tax` as `Salary  0.2`.

Output:
```
| EmployeeID | Name   | Salary | Tax   |
|------------|--------|--------|-------|
| 1          | John   | 60000  | 12000 |
```

# 7. External Call
Scenario: Validate employee data against an external API.

Input:
```
| EmployeeID | Name   | Salary |
|------------|--------|--------|
| 1          | John   | 60000  |
```

Transformation: Call external API to validate `EmployeeID`.

Output:
```
| EmployeeID | Name   | Salary | IsValid |
|------------|--------|--------|---------|
| 1          | John   | 60000  | true    |
```

# 8. Exists
Scenario: Check if a department exists for each employee.

Input:
- Employee Table:
  ```
  | EmployeeID | Name   | DepartmentID |
  |------------|--------|--------------|
  | 1          | John   | 1            |
  | 2          | Jane   | 3            |
  ```

- Department Table:
  ```
  | DepartmentID | DepartmentName |
  |--------------|----------------|
  | 1            | HR             |
  | 2            | IT             |
  ```

Transformation: Check if `DepartmentID` exists in the Department table.

Output:
```
| EmployeeID | Name   | DepartmentID | Exists |
|------------|--------|--------------|--------|
| 1          | John   | 1            | true   |
| 2          | Jane   | 3            | false  |
```

# 9. Filter
Scenario: Filter employees with salaries over $55,000.

Input:
```
| EmployeeID | Name   | Salary |
|------------|--------|--------|
| 1          | John   | 60000  |
| 2          | Jane   | 50000  |
```

Transformation: Keep only employees with `Salary > 55000`.

Output:
```
| EmployeeID | Name | Salary |
|------------|------|--------|
| 1          | John | 60000  |
```

# 10. Flatten
Scenario: Flatten a nested structure of employee projects.

Input:
```
| EmployeeID | Name   | Projects                    |
|------------|--------|-----------------------------|
| 1          | John   | ["Project A", "Project B"]  |
```

Transformation: Unroll the `Projects` array into individual rows.

Output:
```
| EmployeeID | Name | Project    |
|------------|------|------------|
| 1          | John | Project A  |
| 1          | John | Project B  |
```

# 11. Flowlet
Scenario: Create a reusable flowlet for calculating bonuses.

Input: 
- Employee records.

Transformation: Define a flowlet that calculates `Bonus` as `Salary  0.1`.

Output: 
- Use the flowlet in multiple data streams to calculate bonuses consistently.

# 12. Join
Scenario: Join employee and department data.

Input:
- Employee Table:
  ```
  | EmployeeID | Name   | DepartmentID |
  |------------|--------|--------------|
  | 1          | John   | 1            |
  | 2          | Jane   | 2            |
  ```

- Department Table:
  ```
  | DepartmentID | DepartmentName |
  |--------------|----------------|
  | 1            | HR             |
  | 2            | IT             |
  ```

Transformation: Join on `DepartmentID`.

Output:
```
| EmployeeID | Name | DepartmentName |
|------------|------|-----------------|
| 1          | John | HR              |
| 2          | Jane | IT              |
```

# 13. Lookup
Scenario: Lookup department names for each employee.

Input:
```
| EmployeeID | Name   | DepartmentID |
|------------|--------|--------------|
| 1          | John   | 1            |
| 2          | Jane   | 2            |
```

Transformation: Lookup department names from the Department table.

Output:
```
| EmployeeID | Name | DepartmentID | DepartmentName |
|------------|------|--------------|-----------------|
| 1          | John | 1            | HR              |
| 2          | Jane | 2            | IT              |
```

# 14. New Branch
Scenario: Apply separate transformations for salary adjustments and personal information updates.

Input:
```
| EmployeeID | Name   | Salary |
|------------|--------|--------|
| 1          | John   | 60000  |
| 2          | Jane   | 50000  |
```

Transformation: Create two branches—one for updating salaries, one for updating names.

Output:
- Salary Adjustments:
  ```
  | EmployeeID | Salary |
  |------------|--------|
  | 1          | 62000  |
  | 2          | 52000  |
  ```

- Personal Info Updates:
  ```
  | EmployeeID | Name   |
  |------------|--------|
  | 1          | John D.|
  | 2          | Jane A.|
  ```

# 15. Parse
Scenario: Parse a JSON string to extract employee details.

Input:
```
| EmployeeID | EmployeeDetails                                    |
|------------|----------------------------------------------------|
| 1          | {"FirstName":"John", "LastName":"Doe", "Salary":60000} |
```

Transformation: Parse `EmployeeDetails` to extract `FirstName`, `LastName`.

Output:
```
| EmployeeID | FirstName | LastName | Salary |
|------------|-----------|----------|--------

|
| 1          | John      | Doe      | 60000  |
```

# 16. Pivot
Scenario: Pivot employee performance ratings over several years.

Input:
```
| EmployeeID | Year | Rating |
|------------|------|--------|
| 1          | 2022 | 4      |
| 1          | 2023 | 5      |
| 2          | 2022 | 3      |
```

Transformation: Pivot `Year` into columns.

Output:
```
| EmployeeID | 2022 | 2023 |
|------------|------|------|
| 1          | 4    | 5    |
| 2          | 3    | NULL |
```

# 17. Rank
Scenario: Rank employees based on salary.

Input:
```
| EmployeeID | Name   | Salary |
|------------|--------|--------|
| 1          | John   | 60000  |
| 2          | Jane   | 80000  |
| 3          | Mark   | 70000  |
```

Transformation: Rank by `Salary` in descending order.

Output:
```
| EmployeeID | Name   | Salary | Rank |
|------------|--------|--------|------|
| 2          | Jane   | 80000  | 1    |
| 3          | Mark   | 70000  | 2    |
| 1          | John   | 60000  | 3    |
```

# 18. Select
Scenario: Select only relevant columns and rename.

Input:
```
| EmployeeID | Name   | Salary |
|------------|--------|--------|
| 1          | John   | 60000  |
| 2          | Jane   | 80000  |
```

Transformation: Select `EmployeeID` and alias `Name` to `FullName`.

Output:
```
| EmployeeID | FullName | Salary |
|------------|----------|--------|
| 1          | John     | 60000  |
| 2          | Jane     | 80000  |
```

# 19. Sink
Scenario: Write processed employee data to a database.

Input: Processed Employee records.

Transformation: Store records in a database table.

Output: Records saved in `ProcessedEmployees` table.

# 20. Sort
Scenario: Sort employees by name.

Input:
```
| EmployeeID | Name   | Salary |
|------------|--------|--------|
| 1          | John   | 60000  |
| 3          | Mark   | 70000  |
| 2          | Jane   | 80000  |
```

Transformation: Sort by `Name` in ascending order.

Output:
```
| EmployeeID | Name   | Salary |
|------------|--------|--------|
| 2          | Jane   | 80000  |
| 1          | John   | 60000  |
| 3          | Mark   | 70000  |
```

# 21. Source
Scenario: Define data source for employee data.

Transformation: Connect to Employee database to pull records.

Output: Data flow starts with employee records from the source.

# 22. Stringify
Scenario: Convert complex types into strings.

Input:
```
| EmployeeID | Name   | Projects                        |
|------------|--------|----------------------------------|
| 1          | John   | ["Project A", "Project B"]      |
```

Transformation: Stringify `Projects` into a single string.

Output:
```
| EmployeeID | Name   | Projects                  |
|------------|--------|---------------------------|
| 1          | John   | "Project A, Project B"    |
```

# 23. Surrogate Key
Scenario: Add a surrogate key for employee records.

Input:
```
| EmployeeID | Name   | Salary |
|------------|--------|--------|
| 1          | John   | 60000  |
| 2          | Jane   | 80000  |
```

Transformation: Add a new column `SurrogateKey` that increments.

Output:
```
| SurrogateKey | EmployeeID | Name   | Salary |
|--------------|------------|--------|--------|
| 1            | 1          | John   | 60000  |
| 2            | 2          | Jane   | 80000  |
```

# 24. Union
Scenario: Combine employee data from two sources.

Input:
- Source 1:
  ```
  | EmployeeID | Name   | Salary |
  |------------|--------|--------|
  | 1          | John   | 60000  |
  ```

- Source 2:
  ```
  | EmployeeID | Name   | Salary |
  |------------|--------|--------|
  | 2          | Jane   | 80000  |
  ```

Transformation: Union both sources.

Output:
```
| EmployeeID | Name   | Salary |
|------------|--------|--------|
| 1          | John   | 60000  |
| 2          | Jane   | 80000  |
```

# 25. Unpivot
Scenario: Unpivot yearly sales data into a more normalized form.

Input:
```
| EmployeeID | Q1 | Q2 | Q3 | Q4 |
|------------|----|----|----|----|
| 1          | 100| 200| 150| 250|
```

Transformation: Unpivot quarters into rows.

Output:
```
| EmployeeID | Quarter | Sales |
|------------|---------|-------|
| 1          | Q1      | 100   |
| 1          | Q2      | 200   |
| 1          | Q3      | 150   |
| 1          | Q4      | 250   |
```

# 26. Window
Scenario: Calculate a rolling average salary over a 3-month window.

Input:
```
| EmployeeID | Month | Salary |
|------------|-------|--------|
| 1          | Jan   | 60000  |
| 1          | Feb   | 62000  |
| 1          | Mar   | 58000  |
```

Transformation: Compute the average salary over the last three months.

Output:
```
| EmployeeID | Month | Salary | RollingAverage |
|------------|-------|--------|----------------|
| 1          | Jan   | 60000  | 60000          |
| 1          | Feb   | 62000  | 61000          |
| 1          | Mar   | 58000  | 60000          |
```