terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
  }
}

provider "kubernetes" {
  config_path = "~/.kube/config"
}

resource "kubernetes_namespace" "ai_sre" {
  metadata {
    name = "ai-sre"
    labels = {
      project     = "ai-sre-platform"
      environment = "dev"
    }
  }
}

variable "cpu_spike_replicas" {
  description = "Number of replicas for cpu-spike-service"
  type        = number
  default     = 1
}