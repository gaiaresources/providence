<?php

class Paginator {
	private static ?Paginator $instance = null;
	private ?IWLPlugSearchEngineResult $result = null;
	private bool $calculateHits = false;
	private ?int $numHits = null;

	private function __construct(?IWLPlugSearchEngineResult $result) {
		$this->setResult($result);
	}

	public static function getInstance(?IWLPlugSearchEngineResult $result = null): Paginator {
		if (self::$instance === null) {
			self::$instance = new Paginator($result);
		}
		if ($result && $result !== self::$instance->result) {
			self::$instance->setResult($result);
		}

		return self::$instance;
	}

	public function getResult(): ?IWLPlugSearchEngineResult {
		return $this->result;
	}

	public function setResult(?IWLPlugSearchEngineResult $result): Paginator {
		$this->result = $result;
		$this->numHits = null; // Reset numHits when result changes

		return $this;
	}

	public function numHits(): int {
		if (!$this->calculateHits && $this->numHits !== null) {
			return $this->numHits;
		} else {
			$result = $this->getResult();
			if ($result !== null) {
				return count($result->getHits());
			}
		}

		return 0;
	}

	/**
	 * @param bool $calculate
	 *
	 * Override whether we're going to calculate the number of hits rather than getting the number of hits from the
	 *     search directly (default behaviour)
	 *
	 * @return $this
	 */
	public function setCalculateHits(bool $calculate = true): Paginator {
		$this->calculateHits = $calculate;

		return $this;
	}

	public function setNumHits(int $numHits): Paginator {
		$this->numHits = $numHits;

		return $this;
	}
}
