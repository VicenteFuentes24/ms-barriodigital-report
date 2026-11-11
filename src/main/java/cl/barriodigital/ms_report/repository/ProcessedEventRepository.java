package cl.barriodigital.ms_report.repository;

import cl.barriodigital.ms_report.entity.ProcessedEvent;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, UUID> { }
