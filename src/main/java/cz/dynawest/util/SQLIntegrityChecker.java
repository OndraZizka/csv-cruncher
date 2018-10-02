package cz.dynawest.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

public class SQLIntegrityChecker
{
    Logger log = Logger.getLogger(this.getClass().getName());

    public void checkIntegrity(RelationInfo ri)
    {
        List asQueries = this.getCheckSql(ri);
        Iterator iter = asQueries.iterator();

        while (iter.hasNext()) {
            String query = (String) iter.next();
            System.out.println(query + ";");
        }

    }

    public List getCheckSql(RelationInfo ri)
    {
        String sSql = null;
        ArrayList asQueries = new ArrayList(2);
        boolean checkNonExistingParent = false;
        boolean checkNonExistingChild = false;
        switch (ri.type) {
            case REL_1_01:
                checkNonExistingParent = true;
                break;
            case REL_1_1:
                checkNonExistingParent = true;
                checkNonExistingChild = true;
                break;
            case REL_1_0N:
                checkNonExistingParent = true;
        }

        if (checkNonExistingParent) {
            sSql = String.format("SELECT table2.`%4$s` AS has_no_parent, table2.`%5$s` AS referenced_parent \n FROM `%1$s` AS table1 RIGHT JOIN `%2$s` AS table2 ON table1.`%3$s` = table2.`%5$s` \n WHERE table2.`%5$s` IS NOT NULL AND table1.`%3$s` IS NULL", new Object[]{ri.parentTable, ri.childTable, ri.parentId, ri.childId, ri.childFK});
            asQueries.add(sSql);
        }

        if (checkNonExistingChild) {
            sSql = String.format("SELECT table2.`%3$s` AS has_no_child, table2.`%4$s` AS referenced_child \n FROM `%1$s` AS table1 LEFT JOIN `%2$s` AS table2 ON table1.`%3$s` = table2.`%5$s` \n WHERE table2.`%4$s` IS NULL", new Object[]{ri.parentTable, ri.childTable, ri.parentId, ri.childId, ri.childFK});
            asQueries.add(sSql);
        }

        return asQueries;
    }
}
