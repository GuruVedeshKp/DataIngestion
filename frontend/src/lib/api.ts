/**
 * API client for Data Ingestion Pipeline backend
 */

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export interface JobStatus {
  job_id: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  progress: number;
  message: string;
  filename?: string;
  action?: string;
  result?: any;
  error?: string;
  created_at: string;
  updated_at: string;
  timestamp?: string;
  total_records?: number;
  good_records?: number;
  quarantined_records?: number;
  quality_score?: number;
  validation_issues?: number;
}

export interface UploadResponse {
  job_id: string;
  status: string;
  message: string;
}

export interface ValidationResult {
  total_records: number;
  valid_records: number;
  invalid_records: number;
  success_rate: string;
  statistics: any;
  report_path: string;
  schema_info?: any;
}

class ApiClient {
  private baseURL: string;

  constructor(baseURL: string = API_BASE_URL) {
    this.baseURL = baseURL;
  }

  async request<T>(endpoint: string, options: RequestInit = {}): Promise<T> {
    const url = `${this.baseURL}${endpoint}`;
    
    const response = await fetch(url, {
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
      ...options,
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(errorData.detail || `HTTP ${response.status}: ${response.statusText}`);
    }

    return response.json();
  }

  // Health check
  async healthCheck() {
    return this.request('/api/health');
  }

  // File upload
  async uploadFile(file: File, action: 'validate' | 'kafka' | 'pipeline' = 'validate'): Promise<UploadResponse> {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('action', action);

    const response = await fetch(`${this.baseURL}/api/upload`, {
      method: 'POST',
      body: formData,
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(errorData.detail || `Upload failed: ${response.statusText}`);
    }

    return response.json();
  }

  // Get job status
  async getJobStatus(jobId: string): Promise<JobStatus> {
    return this.request(`/api/status/${jobId}`);
  }

  // List all jobs
  async listJobs(): Promise<{ jobs: JobStatus[]; count: number }> {
    return this.request('/api/jobs');
  }

  // Delete job
  async deleteJob(jobId: string): Promise<{ message: string }> {
    return this.request(`/api/jobs/${jobId}`, { method: 'DELETE' });
  }

  // Download file
  async downloadFile(jobId: string, fileType: 'report' | 'good_data' | 'all_data'): Promise<Blob> {
    const response = await fetch(`${this.baseURL}/api/download/${jobId}/${fileType}`);
    
    if (!response.ok) {
      throw new Error(`Download failed: ${response.statusText}`);
    }

    return response.blob();
  }

  // Get quarantined items
  async getQuarantinedItems(): Promise<{quarantined_items: any[], count: number}> {
    const response = await fetch(`${this.baseURL}/api/quarantine`);
    
    if (!response.ok) {
      throw new Error(`Failed to fetch quarantined items: ${response.statusText}`);
    }

    return response.json();
  }

  // Get system statistics
  async getStatistics(): Promise<any> {
    const response = await fetch(`${this.baseURL}/api/statistics`);
    
    if (!response.ok) {
      throw new Error(`Failed to fetch statistics: ${response.statusText}`);
    }

    return response.json();
  }

  // Poll job status until completion
  async pollJobStatus(jobId: string, onProgress?: (status: JobStatus) => void): Promise<JobStatus> {
    return new Promise((resolve, reject) => {
      const poll = async () => {
        try {
          const status = await this.getJobStatus(jobId);
          
          if (onProgress) {
            onProgress(status);
          }

          if (status.status === 'completed') {
            resolve(status);
          } else if (status.status === 'failed') {
            reject(new Error(status.error || 'Job failed'));
          } else {
            // Continue polling
            setTimeout(poll, 1000);
          }
        } catch (error) {
          reject(error);
        }
      };

      poll();
    });
  }
}

export const apiClient = new ApiClient();
export default apiClient;
