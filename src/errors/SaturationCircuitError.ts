export class SaturationCircuitError extends Error {
  public readonly isSaturationCircuitError = true;
  /**
   * Exception thrown from {@link CircuitBreakerPolicy.execute} when the
   * circuit breaker is half open and requests for probing the circuit
   * has exceeded the specified limit in {@link IHalfOpenBreaker}.
   */
  constructor(message = 'Execution prevented because the circuit breaker is busy to probe the half open connection') {
    super(message);
  }
}
