package org.apache.oozie.util.db;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "VALIDATE_CONN")
public class ValidateConnectionBean {

    @Basic
    @Column
    public int dummy;

}
