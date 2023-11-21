ALTER TABLE departements
ADD CONSTRAINT departements_num_dep_key
UNIQUE (num_dep);

ALTER TABLE tranches_dage
ADD CONSTRAINT tranches_dage_code_tranches_dage_key
UNIQUE (code_tranches_dage);

ALTER TABLE passages_corona
ADD CONSTRAINT passages_corona_dep_fkey
FOREIGN KEY (dep)
REFERENCES departements (num_dep);

ALTER TABLE passages_corona
ADD CONSTRAINT passages_corona_sursaud_cl_age_corona_fkey
FOREIGN KEY (sursaud_cl_age_corona)
REFERENCES tranches_dage (code_tranches_dage);
