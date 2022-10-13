// Implementación de la interfaz de un nodo de datos

package datanode;
import java.util.List;
import java.util.Iterator;

import java.rmi.*;
import java.rmi.server.*;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;


import interfaces.*;

public class DataNodeImpl extends UnicastRemoteObject implements DataNode {
    private String name;
    private Manager manager;
    private int chunksize;
    LockManager lm;

    public static final long  serialVersionUID=1234567891;

    public DataNodeImpl(Manager m, String n) throws RemoteException {
    // FASE 1
    this.name = n;
    this.manager = m;
    m.addDataNode(this);
    lm = new LockManager();
    }
    // nombre del nodo
    public String getName() throws RemoteException {
        return this.name;
    }
    // lee el fichero que contiene un chunk
    public byte [] readChunk(String chunkName) throws RemoteException {
        byte [] res  = null;
        try{
            String path = "../bin/"+ this.name + "/" + chunkName;
            FileInputStream  fichero = new FileInputStream(path);
            res = fichero.readAllBytes();
            fichero.read(res);
            fichero.close();            
        }catch(IOException IOe) {
                IOe.printStackTrace();
                res = null;
        }
        return res;
    }
    // escribe en un fichero local el contenido del chunk;
    // si la lista de nodos pasada como parámetro no esta vacía,
    // propaga la escritura a los nodos de datos de la lista
    public boolean writeChunk(List <DataNode> nodes, String chunkName, byte [] buffer) throws RemoteException {
        boolean res = true;
        if(nodes == null || nodes.isEmpty()){
            //../bin/this.name/chunkName
            try{
            String path = "../bin/"+ this.name + "/" + chunkName;
            FileOutputStream fichero = new FileOutputStream(path);
            fichero.write(buffer);
            fichero.close();
            } catch(IOException IOe) {
                IOe.printStackTrace();
                res = false;
            } 
        }
        else{            
            LockManager.Lock lck;
            (lck = lm.openLock(chunkName)).lock();
            this.writeChunk(null,chunkName,buffer);
            Iterator<DataNode> nodosIt = nodes.iterator();
            while(nodosIt.hasNext()){
                DataNode n = nodosIt.next();
                //System.out.println(""+i+" " +n.getName());
                res&=n.writeChunk(null,chunkName,buffer);       
            }
            lck.unlock(); 
        }
        return res;
    }
}
