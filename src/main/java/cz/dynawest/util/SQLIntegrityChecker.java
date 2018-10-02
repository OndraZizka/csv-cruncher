package cz.dynawest.util;

import cz.dynawest.util.RelationInfo;
import cz.dynawest.util.RelationInfo.Type;
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
        Iterator i$ = asQueries.iterator();

        while (i$.hasNext()) {
            String query = (String) i$.next();
            System.out.println(query + ";");
        }

    }

    public List getCheckSql(RelationInfo ri)
    {
        String sSql = null;
        ArrayList asQueries = new ArrayList(2);
        boolean checkNonExistingParent = false;
        boolean checkNonExistingChild = false;
        switch (SQLIntegrityChecker.SyntheticClass_1.$SwitchMap$cz$dynawest$util$RelationInfo$Type[ri.type.ordinal()]) {
            case 1:
                checkNonExistingParent = true;
                break;
            case 2:
                checkNonExistingParent = true;
                checkNonExistingChild = true;
                break;
            case 3:
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


    // $FF: synthetic class
    static class SyntheticClass_1
    {
        // $FF: synthetic field
        static final int[] $SwitchMap$cz$dynawest$util$RelationInfo$Type = new int[Type.values().length];

        static {
            try {
                $SwitchMap$cz$dynawest$util$RelationInfo$Type[Type.REL_1_01.ordinal()] = 1;
            }
            catch (NoSuchFieldError var3) {
                ;
            }

            try {
                $SwitchMap$cz$dynawest$util$RelationInfo$Type[Type.REL_1_1.ordinal()] = 2;
            }
            catch (NoSuchFieldError var2) {
                ;
            }

            try {
                $SwitchMap$cz$dynawest$util$RelationInfo$Type[Type.REL_1_0N.ordinal()] = 3;
            }
            catch (NoSuchFieldError var1) {
                ;
            }

        }
    }
}
