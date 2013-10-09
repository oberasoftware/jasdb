package nl.renarj.jasdb.index.btreeplus.persistence;

/**
 * @author Renze de Vries
 */
public enum BlockTypes {
    ROOTBLOCK(4),
    NODEBLOCK(8),
    LEAVEBLOCK(16),
    DATA(32);

    private int typeDef;

    BlockTypes(int typeDef) {
        this.typeDef = typeDef;
    }

    public int getTypeDef() {
        return typeDef;
    }

    public static BlockTypes getByTypeDef(int typeDef) {
        if(typeDef == 32) {
            //most often hit for load, so first case
            return DATA;
        } else if(typeDef == 16) {
            return LEAVEBLOCK;
        } else if(typeDef == 8) {
            return NODEBLOCK;
        } else if(typeDef == 4){
            return ROOTBLOCK;
        } else {
            throw new RuntimeException("Unrecognized block type");
        }
    }
}
