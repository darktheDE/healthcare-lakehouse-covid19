-- Tạo bảng patients
CREATE TABLE patients (
    id UUID PRIMARY KEY,
    birthdate DATE NOT NULL,
    deathdate DATE,
    ssn VARCHAR(20),
    drivers VARCHAR(20),
    passport VARCHAR(20),
    prefix VARCHAR(20),
    first VARCHAR(100),
    last VARCHAR(100),
    suffix VARCHAR(20),
    maiden VARCHAR(100),
    marital VARCHAR(1),
    race VARCHAR(50),
    ethnicity VARCHAR(50),
    gender VARCHAR(1),
    birthplace VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    county VARCHAR(100),
    zip VARCHAR(20),
    lat NUMERIC(10, 6),
    lon NUMERIC(10, 6),
    healthcare_expenses NUMERIC(12, 2),
    healthcare_coverage NUMERIC(12, 2)
);

-- Tạo bảng encounters
CREATE TABLE encounters (
    id UUID PRIMARY KEY,
    start_time TIMESTAMPTZ NOT NULL,
    stop_time TIMESTAMPTZ,
    patient UUID REFERENCES patients(id),
    organization UUID,
    provider UUID,
    payer UUID,
    encounterclass VARCHAR(50),
    code VARCHAR(50),
    description VARCHAR(255),
    base_encounter_cost NUMERIC(12, 2),
    total_claim_cost NUMERIC(12, 2),
    payer_coverage NUMERIC(12, 2),
    reasoncode VARCHAR(50),
    reasondescription VARCHAR(255)
);

-- Tạo bảng conditions
CREATE TABLE conditions (
    start_date DATE NOT NULL,
    stop_date DATE,
    patient UUID REFERENCES patients(id),
    encounter UUID REFERENCES encounters(id),
    code VARCHAR(50),
    description VARCHAR(255)
);
