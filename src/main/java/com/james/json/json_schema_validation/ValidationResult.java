package com.james.json.json_schema_validation;

public class ValidationResult {
	private boolean success = false;
	private String errorMessage = null;
	
	public ValidationResult() {
		
	}

	public boolean isSuccess() {
		return success;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	@Override
	public String toString() {
		return "ValidationResult [success=" + success + ", errorMessage=" + errorMessage + "]";
	}
	
}
