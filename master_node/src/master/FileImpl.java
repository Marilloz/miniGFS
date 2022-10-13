// Implementación de la interfaz remota para el acceso a la información de ubicación de un fichero

package master;
import java.util.List;
import java.util.ArrayList;
import java.rmi.*;
import java.rmi.server.*;
import manager.*;
import interfaces.*;

import java.util.Map;
import java.util.HashMap;

public class FileImpl extends UnicastRemoteObject implements File {
    private Map<Integer,ChunkImpl> mapaChunk;
    private ManagerImpl manager;
    public static final long  serialVersionUID=1234567891;
    private int max;
    private int replicas;

    public FileImpl(ManagerImpl m, int replicaFactor) throws RemoteException {
        this.manager = m;
        this.max = 0;
        this.replicas = replicaFactor;
        mapaChunk = new HashMap<Integer,ChunkImpl>();        
    }
    // nº de chunks del fichero
    public int getNumberOfChunks() throws RemoteException {
        return max;
    }
    // obtiene información de ubicación de los chunks especificados del fichero
    public List <Chunk> getChunkDescriptors(int nchunk, int size) throws RemoteException {
        List <Chunk> chunks  = new ArrayList<Chunk>();
        System.out.println("MAX = " + max);
        for(int i = nchunk; i< size + nchunk && i < max; i++){
            if(mapaChunk.containsKey(i)){
                ChunkImpl c = mapaChunk.get(i);
                chunks.add(c);
            }
            else{
                chunks.add(null);
            }
        }
        return chunks;
    }
    
    private int max(int x, int y){
        if(x >= y) return x;
        else return y;
    }
    
    // reserva información de ubicación para los chunks especificados del fichero
    public List <Chunk> allocateChunkDescriptors(int nchunk, int size) throws RemoteException {
        List <Chunk> chunks  = new ArrayList<Chunk>();
        for(int i = nchunk; i< size + nchunk; i++){
            if(mapaChunk.containsKey(i)){ 
                mapaChunk.remove(i); // AL SOBREESCRIBIR BORRO Y AÑADO DESPUES      
            }
            
            List<DataNode> nodosDatos = new ArrayList<DataNode>();
            for(int j=0; j<this.replicas; j++){
                DataNode d =  this.manager.selectDataNode();
                nodosDatos.add(d);
            }
            ChunkImpl c  = new ChunkImpl(nodosDatos);
            mapaChunk.put(i,c);
            chunks.add(c);
            System.out.println(i);
            max = max(max,i+1);
        }
        return chunks;
    }
}
