// Implementación de la interfaz del manager

package manager;
import java.util.List;
import java.util.ArrayList;
import java.rmi.*;
import java.rmi.server.*;

import interfaces.*;

public class ManagerImpl extends UnicastRemoteObject implements Manager {
    private List<DataNode> nodosDatos;
    private int indiceCiclo;

    public static final long  serialVersionUID=1234567891;

    public ManagerImpl() throws RemoteException {
        nodosDatos = new ArrayList<DataNode>();
        indiceCiclo = 0;
    }
    // alta de un nodo de datos
    public synchronized void addDataNode(DataNode n) throws RemoteException {
        nodosDatos.add(n);
    }
    // obtiene lista de nodos de datos del sistema
    public synchronized List <DataNode> getDataNodes() throws RemoteException {
        return nodosDatos;
    }
    // método no remoto que selecciona un nodo de datos para ser usado
    // para almacenar un chunk
    public synchronized DataNode selectDataNode() {
        DataNode n = nodosDatos.get(indiceCiclo);
        indiceCiclo++;
        if(indiceCiclo == nodosDatos.size()){
            indiceCiclo = 0;
        }
        return n;
    }
}
