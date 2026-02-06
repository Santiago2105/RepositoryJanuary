package pe.com.comsatel.clocator.microservice.posiciones.domain.internal;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import pe.com.comsatel.clocator.microservice.posiciones.domain.DomainReplicasService;
import pe.com.comsatel.clocator.microservice.posiciones.domain.ReplicasRepository;
import pe.com.comsatel.clocator.microservice.posiciones.domain.model.ReplicaLogResponse;

/**
 *
 * @author ianache
 */

@Service
public class ReplicasServiceImpl implements DomainReplicasService {

    private static final Logger LOG = LogManager.getLogger(ReplicasServiceImpl.class);

    @Autowired
    @Qualifier("MYSQL")
    private ReplicasRepository domainRepository;

    @Override
    public List<ReplicaLogResponse> listarLogPorCompania(String companiaId, String fechaInicio, String fechaFin,
            String criterio1, String criterio2) {
        return domainRepository.listarLogPorCompania(companiaId, fechaInicio, fechaFin, criterio1, criterio2);
    }

}
