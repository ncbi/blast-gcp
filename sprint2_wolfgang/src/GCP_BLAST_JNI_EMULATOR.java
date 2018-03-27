import java.io.*;
import java.util.Random;

class GCP_BLAST_JNI_EMULATOR implements Serializable
{
    private Random rand;
    
    public GCP_BLAST_JNI_EMULATOR()
    {
        rand = new Random();
    }
    
    public ArrayList< GCP_BLAST_HSP > make_hsp( final GCP_BLAST_JOB job, Integer count, Long oid )
    {
        ArrayList< GCP_BLAST_HSP > res = new ArrayList<>();
        
        for ( int i = 1; i < count; i++ )
            partitions.add( new GCP_BLAST_HSP( job, oid ) );
            
        return res;
    }
}
