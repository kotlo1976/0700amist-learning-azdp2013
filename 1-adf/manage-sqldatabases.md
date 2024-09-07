-- Create the Department table
CREATE TABLE Department (
    DepartmentID INT PRIMARY KEY,
    DepartmentName VARCHAR(255) NOT NULL
);

-- Insert sample values into the Department table
INSERT INTO Department (DepartmentID, DepartmentName)
VALUES
    (1, 'Human Resources'),
    (2, 'Finance'),
    (3, 'Sales'),
    (4, 'Marketing'),
    (5, 'Engineering');

-- Create the Employee table
CREATE TABLE Employee (
    EmployeeID INT PRIMARY KEY,
    FirstName VARCHAR(50) NOT NULL,
    LastName VARCHAR(50) NOT NULL,
    DepartmentID INT,
    Salary DECIMAL(10, 2),
    HireDate DATE
);

-- Insert sample values into the Employee table
INSERT INTO Employee (EmployeeID, FirstName, LastName, DepartmentID, Salary, HireDate)
VALUES
    (1, 'John', 'Doe', 3, 60000.00, '2020-01-15'),
    (2, 'Jane', 'Smith', 2, 55000.00, '2019-05-20'),
    (3, 'Robert', 'Johnson', 4, 62000.00, '2021-03-10'),
    (4, 'Emily', 'Williams', 1, 58000.00, '2018-09-03'),
    (5, 'Michael', 'Brown', 5, 65000.00, '2022-02-28');
