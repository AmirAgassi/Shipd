import hashlib
import uuid
from datetime import datetime
from typing import Dict, List
from hashlib import sha256


class DistributedSecretManager:
    def __init__(self):
        self.services: Dict[str, Dict] = {}  # service_id -> service_info
        self.roles_permissions: Dict[str, List[str]] = {}  # role -> list of permissions
        self.audit_logs: List[Dict] = []  # Audit logs for tracking events
        self.secrets: Dict[str, str] = {}  # service_id -> hashed_secret

    def _check_permissions(self, service_id: str, permission: str):
        """
        Check if the service has the required permisison based on its role.
        Raises PermissionError if the permission is not granted.
        """
        if service_id not in self.services:
            raise ValueError(f"Service '{service_id}' is not registered.")
        role = self.services[service_id]["role"]
        if permission not in self.roles_permissions.get(role, []):
            # Log permission denial for auditing purposes
            self.audit_event("PERMISSION_DENIED", service_id, f"Permission '{permission}' denied for role '{role}'")
            raise PermissionError(f"Service '{service_id}' does not have '{permission}' permission.")

    def register_service(self, service_id: str, role: str):
        """
        Register a new service with a specific role.
        Assigns a new secret to the service.
        """
        # Check if the service is already registered
        if service_id in self.services:
            raise ValueError(f"Service '{service_id}' is already registered.")

        # Check if the role exists
        if role not in self.roles_permissions:
            raise ValueError(f"Role '{role}' does not exist.")

        # Generate a new secret for the service
        plaintext_secret = str(uuid.uuid4())
        self.secrets[service_id] = plaintext_secret
        self.services[service_id] = {"role": role, "plaintext_secret": plaintext_secret}

        # Log the registration event
        self.audit_event("REGISTER", service_id, f"Service '{service_id}' registered with role '{role}'")

    def deregister_service(self, service_id: str):
        """
        Remove a service and its associated secret from the system.
        """
        if service_id not in self.services:
            raise ValueError(f"Service '{service_id}' is not registered.")

        # Remove the service and its secret
        del self.services[service_id]
        del self.secrets[service_id]

        # Log the deregistration event
        self.audit_event("DEREGISTER", service_id, f"Service '{service_id}' deregistered.")

    def authenticate_service(self, service_id: str, secret: str):
        """
        Authenticate a service by checking its secret.
        Returns True if the secret matches; False otherwise.
        """
        if service_id not in self.services:
            return False
        hashed_secret = sha256(secret.encode()).hexdigest()
        return self.secrets.get(service_id) == secret

    def get_secret(self, service_id: str, requester_id: str):
        """
        Allow a service to retrieve its own secret. Requires 'access_secret' permission.
        """
        if service_id not in self.services:
            raise ValueError(f"Service '{service_id}' is not registered.")

        # Check if the requester has permission to access secrets
        self._check_permissions(requester_id, "access_secret")

        # Log the access event
        self.audit_event("ACCESS", requester_id, f"Accessed secret of '{service_id}'")

        # Return the secret
        return self.services[service_id]["plaintext_secret"]

    def rotate_secret(self, service_id: str):
        """
        Rotate the secret for a specific service, generating a new one.
        """
        if service_id not in self.services:
            raise ValueError(f"Service '{service_id}' is not registered.")

        # Generate a new secret
        new_secret = str(uuid.uuid4())
        self.secrets[service_id] = new_secret
        self.services[service_id]["plaintext_secret"] = new_secret

        # Log the rotation event
        self.audit_event("ROTATE", service_id, f"Secret rotated for '{service_id}'")

    def rotate_all_secrets(self, requester_id: str, requester_secret: str):
        """
        Rotate the secrets for all services. Requires 'rotate_secret' permission.
        """
        # Authenticate the requester
        if not self.authenticate_service(requester_id, requester_secret):
            self.audit_event("BULK_ROTATION_DENIED", requester_id, "Failed to authenticate for bulk rotation.")
            raise PermissionError("Authentication failed for requester.")

        # Check if the requester has permission to rotate all secrets
        self._check_permissions(requester_id, "rotate_secret")

        # Rotate secrets for all services
        for sid in self.services.keys():
            try:
                self.rotate_secret(sid)
            except Exception as e:
                self.audit_event("ROTATE_FAILED", sid, f"Rotation failed: {str(e)}")

        # Log the bulk rotation event
        self.audit_event("ALL_SECRETS_ROTATED", requester_id, "All secrets have been rotated successfully.")

    def add_role(self, role: str, permissions: List[str]):
        """
        Add a new role with specific permissions.
        """
        self.roles_permissions[role] = permissions
        # Log the role addition event
        self.audit_event("ROLE_ADD", "SYSTEM", f"Role '{role}' added with permissions: {permissions}")

    def remove_role(self, role: str):
        """
        Remove an existing role. Logs an event if the role does not exist.
        """
        if role in self.roles_permissions:
            del self.roles_permissions[role]
            # Log the successful removal event
            self.audit_event("REMOVE_ROLE", "SYSTEM", f"Role '{role}' removed")
        else:
            # Log the failiure to remove the role
            self.audit_event("REMOVE_ROLE_FAILED", "SYSTEM", f"Role '{role}' does not exist")

    def audit_event(self, event_type: str, service_id: str, details: str):
        """
        Record an audit event with a timestamp, event type, service ID, and details.
        """
        self.audit_logs.append(
            {
                "timestamp": datetime.utcnow().isoformat(),
                "event_type": event_type,
                "service_id": service_id,
                "details": details,
            }
        )
