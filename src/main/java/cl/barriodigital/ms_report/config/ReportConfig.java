package cl.barriodigital.ms_report.config;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ReportConfig {
    @Bean
    Clock clock() { return Clock.systemUTC(); }
}
