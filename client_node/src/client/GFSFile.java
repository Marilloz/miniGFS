// Clase de cliente que proporciona los métodos para acceder a los ficheros.
// Corresponde al API ofrecida a las aplicaciones 

package client;
import java.rmi.*;
import interfaces.*;
import java.util.List;
import java.util.Arrays;

public class GFSFile {
    private String host;
    private String port;
    private String fileName;

    private int chunksize;
    private Master master;
    private Manager manager;
    private int offset;

    public GFSFile(String fileName){
        this.fileName = fileName;
        this.port = System.getenv("BROKER_PORT");
        this.host = System.getenv("BROKER_HOST");
        this.chunksize =  Integer.parseInt(System.getenv("CHUNKSIZE"));

        try {
            this.master = (Master) Naming.lookup("//" +  this.host + ":" + this.port + "/GFS_master");
            this.manager = (Manager) Naming.lookup("//" +  this.host + ":" + this.port + "/GFS_manager");
            System.out.println(this.manager.getDataNodes().size());
        }catch(Exception e){
            e.printStackTrace();
        }

    }
    // establece la posición de acceso al fichero
    public void seek(int off) {
        this.offset = off;
    }
    // obtiene la posición de acceso al fichero
    public int getFilePointer() {
        return this.offset;
    }
    // obtiene la longitud del fichero en bytes
    public int length() throws RemoteException {
        File f = this.master.lookup(this.fileName);     
        return f.getNumberOfChunks()*this.chunksize;
    }
    // lee de la posición actual del fichero la cantidad de datos pedida;
    // devuelve cantidad realmente leída, 0 si EOF;
    // actualiza la posición de acceso al fichero
    public int read(byte [] buf) throws RemoteException {
        int chunksALeer = buf.length / this.chunksize;        
        int chunkActual = this.offset / this.chunksize;
        int res = 0;
        byte cero = '\0';

        File fich = this.master.lookup(this.fileName);
        List <Chunk> listaChunks = fich.getChunkDescriptors(chunkActual,chunksALeer);
        System.out.println(listaChunks);
        int buffIndex = 0;
        for(int i=0; i<listaChunks.size(); i++){
            Chunk c = listaChunks.get(i);
            System.out.print(i);
            if(c != null){
                System.out.println(" NOT NULL");
                List <DataNode> nodos = c.getChunkDataNodes();
                DataNode primario = nodos.get(0);
                byte []datos = primario.readChunk(c.getChunkName());              
                System.arraycopy(datos,0,buf,buffIndex,this.chunksize);

                buffIndex+=chunksize;
                res+=this.chunksize;
            }
            else{
                System.out.println(" IS NULL");
                int newBuffIndex = buffIndex + this.chunksize;
                Arrays.fill(buf,buffIndex,newBuffIndex,cero);
                buffIndex = newBuffIndex;
                res+=this.chunksize;
            }


        }

        this.offset += buf.length;
        return res;
    }
    // escribe en la posición actual del fichero los datos especificados;
    // devuelve falso si se ha producido un error en writeChunk;
    // actualiza la posición de acceso al fichero
    public boolean write(byte [] buf) throws RemoteException {
        int chunksAEscribir = buf.length / this.chunksize;        
        int chunkActual = this.offset / this.chunksize;
        File fich = this.master.lookup(this.fileName);
        List <Chunk> listaChunks = fich.allocateChunkDescriptors(chunkActual,chunksAEscribir);

        int buffIndex = 0;
        boolean res = true;
        for(int i=0; i<listaChunks.size(); i++){
            Chunk c = listaChunks.get(i);
            System.out.println(c.getChunkName());
            List <DataNode> nodos = c.getChunkDataNodes();
            DataNode primario = nodos.get(0);
            nodos.remove(0);
            int newBuffIndex = buffIndex + this.chunksize;
            byte[] datos = Arrays.copyOfRange(buf,buffIndex,newBuffIndex);
            res &= primario.writeChunk(nodos,c.getChunkName(),datos);
            buffIndex = newBuffIndex;
            nodos.add(0,primario);
        }

        this.offset += buf.length;
        return res;
    }
}
