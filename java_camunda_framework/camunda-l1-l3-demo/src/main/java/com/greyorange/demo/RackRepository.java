package com.greyorange.demo;

import org.springframework.data.jpa.repository.JpaRepository;

public interface RackRepository extends JpaRepository<RackEntity, String> {
}
