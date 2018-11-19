package org.springframework.data.r2dbc.repository.config;

import org.springframework.data.r2dbc.repository.R2dbcRepository;

/**
 * @author Mark Paluch
 */
interface PersonRepository extends R2dbcRepository<Person, String> {}
