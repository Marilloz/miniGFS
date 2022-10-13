// Implementación de la interfaz remota Master

package master;
import java.rmi.*;
import java.rmi.server.*;
import manager.*;
import interfaces.*;

import java.util.Map;
import java.util.HashMap;

public class MasterImpl extends UnicastRemoteObject implements Master {
    private Map<String,FileImpl> mapaFicheros;
    private ManagerImpl manager;
    private int replicas;
    public static final long  serialVersionUID=1234567891;

    public MasterImpl(ManagerImpl m, int replica) throws RemoteException {
        this.manager = m;
        this.replicas = replica;
        mapaFicheros = new HashMap<String,FileImpl>();
    }
    // obtiene acceso a la metainformación de un fichero
    public synchronized File lookup(String fname) throws RemoteException {
        FileImpl file;
        if(mapaFicheros.containsKey(fname)){
            file = mapaFicheros.get(fname);
        }
        else{
            file = new FileImpl(manager,this.replicas);
            mapaFicheros.put(fname,file);
        }
        return file;
    }
}
