package dps.common.model;

/**
 * DTO representing the complete status of a production line,
 * including its identity and current operational state.
 */
public record ProductionLineStatus(ProductionLine line, OperationalState state) {}
