drop  table IF EXISTS urgences; 
drop  table IF EXISTS departements; 
drop  table IF EXISTS tranches_dage; 

CREATE TABLE IF NOT EXISTS tranches_dage(
      Code_tranches_dage INT,
      Age VARCHAR(50),
      PRIMARY KEY (code_tranches_dage)
    );

    CREATE TABLE IF NOT EXISTS departements (
    num_dep VARCHAR(10),
    dep_name VARCHAR(50),
    region_name VARCHAR(50),
    PRIMARY KEY (num_dep)
);

CREATE TABLE IF NOT EXISTS urgences (
    dep VARCHAR(10),
    date_de_passage DATE,
    sursaud_cl_age_corona INT,
    nbre_pass_corona INT,
    nbre_pass_tot INT,
    nbre_hospit_corona INT,
    nbre_pass_corona_h INT,
    nbre_pass_corona_f INT,
    nbre_pass_tot_h INT,
    nbre_pass_tot_f INT,
    nbre_hospit_corona_h INT,
    nbre_hospit_corona_f INT,
    FOREIGN KEY (dep) REFERENCES departements(num_dep),
    FOREIGN KEY (sursaud_cl_age_corona) REFERENCES tranches_dage(Code_tranches_dage)
);
