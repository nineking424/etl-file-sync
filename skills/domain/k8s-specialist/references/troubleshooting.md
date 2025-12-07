# Troubleshooting

Common Kubernetes issues and debugging techniques.

## Pod Issues

### Pod Not Starting
```bash
# Check pod status
kubectl get pods

# Describe pod for events
kubectl describe pod <pod-name>

# Check logs
kubectl logs <pod-name>
kubectl logs <pod-name> --previous  # Previous container instance

# Common causes:
# - ImagePullBackOff: Image not found or auth issue
# - CrashLoopBackOff: Container crashes on startup
# - Pending: Insufficient resources or scheduling constraints
```

### Image Pull Errors
```bash
# Check image pull secret
kubectl get secret <secret-name> -o yaml

# Create image pull secret
kubectl create secret docker-registry regcred \
  --docker-server=<registry> \
  --docker-username=<username> \
  --docker-password=<password>

# Add to pod spec
imagePullSecrets:
- name: regcred
```

### Resource Issues
```bash
# Check node resources
kubectl top nodes

# Check pod resources
kubectl top pods

# Describe node to see allocatable resources
kubectl describe node <node-name>

# Solution: Add resources or scale up nodes
```

## Network Issues

### Service Not Reachable
```bash
# Check service
kubectl get svc <service-name>

# Check endpoints
kubectl get endpoints <service-name>

# Verify pod labels match service selector
kubectl get pods --show-labels

# Test connectivity from debug pod
kubectl run -it --rm debug --image=busybox --restart=Never -- wget -O- <service-name>
```

### DNS Not Working
```bash
# Check CoreDNS pods
kubectl get pods -n kube-system -l k8s-app=kube-dns

# Test DNS resolution
kubectl run -it --rm debug --image=busybox --restart=Never -- nslookup kubernetes.default

# Check CoreDNS logs
kubectl logs -n kube-system -l k8s-app=kube-dns
```

## Performance Issues

### High CPU/Memory
```bash
# Check resource usage
kubectl top pods
kubectl top nodes

# Analyze metrics
kubectl describe node <node-name>

# Solutions:
# - Increase resource limits
# - Add HPA for horizontal scaling
# - Optimize application code
```

### OOMKilled Pods
```bash
# Check pod events
kubectl describe pod <pod-name>
# Look for: "Reason: OOMKilled"

# Solution: Increase memory limits
resources:
  limits:
    memory: "2Gi"  # Increase from current value
```

## Debugging Commands

### Essential Commands
```bash
# Get pods in all namespaces
kubectl get pods -A

# Watch pod status
kubectl get pods -w

# Execute command in pod
kubectl exec -it <pod-name> -- /bin/sh

# Port forward to pod
kubectl port-forward <pod-name> 8080:8080

# Copy files to/from pod
kubectl cp <pod-name>:/path/to/file ./local-file
kubectl cp ./local-file <pod-name>:/path/to/file

# Get events
kubectl get events --sort-by='.lastTimestamp'
```

## Best Practices

1. **Always check pod events** with kubectl describe
2. **Review logs** for application errors
3. **Verify resource requests/limits** are appropriate
4. **Check network policies** if connectivity fails
5. **Use debug containers** for troubleshooting
6. **Monitor metrics** continuously
7. **Test in staging** before production
8. **Document issues** and solutions
9. **Use proper logging** and observability
10. **Keep Kubernetes updated** for bug fixes
