


CREATE TABLE IF NOT EXISTS hospitalisation(
    date_passage DATE,
    num_dep VARCHAR(10),
    
    PRIMARY KEY (num_dep, date_passage)
);

CREATE TABLE IF NOT EXISTS passage_urgence(
    num_dep VARCHAR(10),
    Code_tranches_dage VARCHAR(50),
    date_de_passage DATE,
    PRIMARY KEY (num_dep, code_tranches_dage,date_de_passage)
);

CREATE TABLE IF NOT EXISTS departements (
    num_dep VARCHAR(10),
    dep_name VARCHAR(50),
    region_name VARCHAR(50),
    PRIMARY KEY (num_dep)
);

CREATE TABLE IF NOT EXISTS urgences_hosp (
    dep VARCHAR(10),
    date_de_passage DATE,
    sursaud_cl_age_corona VARCHAR(50),
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
    FOREIGN KEY (dep, date_de_passage) REFERENCES hospitalisation(num_dep, date_passage),
    FOREIGN KEY (dep, sursaud_cl_age_corona,date_de_passage) REFERENCES passage_urgence(num_dep, Code_tranches_dage,date_de_passage)
);
