CREATE TABLE classes (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL
);

CREATE TABLE students (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    class_id INTEGER REFERENCES classes(id)
);

CREATE TABLE attendance (
    id SERIAL PRIMARY KEY,
    student_id INTEGER REFERENCES students(id),
    class_id INTEGER REFERENCES classes(id),
    clock_in TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    clock_out TIMESTAMP,
    status VARCHAR(20) -- e.g., 'present', 'late'
);

-- Quick seed for Classes
INSERT INTO classes (name) SELECT 'Class ' || i FROM generate_series(1, 20) i;

-- Quick seed for 300 Students distributed across classes
INSERT INTO students (name, class_id) 
SELECT 'Student ' || i, (i % 20) + 1 
FROM generate_series(1, 300) i;