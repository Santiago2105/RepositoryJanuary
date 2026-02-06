package pe.com.comsatel.clocator.microservice.posiciones.domain.internal;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import pe.com.comsatel.clocator.microservice.posiciones.domain.DomainProveedoresService;
import pe.com.comsatel.clocator.microservice.posiciones.domain.ProveedoresRepository;
import pe.com.comsatel.clocator.microservice.posiciones.domain.model.ProveedorResponse;

/**
 *
 * @author ianache
 */

@Service
public class ProveedoresServiceImpl implements DomainProveedoresService {

    private static final Logger LOG = LogManager.getLogger(ProveedoresServiceImpl.class);

    @Autowired
    @Qualifier("MYSQL")
    private ProveedoresRepository domainRepository;

    @Override
    public List<ProveedorResponse> obtenerPorTag(String tag) {
        return domainRepository.obtenerPorTag(tag);
    }

}
